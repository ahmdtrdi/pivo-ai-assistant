"""
llm.py — Gemini 2.0 Flash wrapper for tier-aware WA message generation.

Input:  structured payload dict (Fig2 schema)
Output: Bahasa Indonesia WhatsApp message (3 sections)

Tone adapts to confidence tier:
  Green  → specific numbers + one concrete action
  Yellow → numbers with caveat ("masih perkiraan awal")
  Red    → encouragement only, no numbers, no strong claims
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """
Kamu adalah asisten AI untuk pemilik usaha kecil (UMKM) di Indonesia.
Tugasmu adalah menulis pesan WhatsApp harian yang berisi insight bisnis,
berdasarkan data penjualan dan prediksi yang sudah dianalisa sistem PIVO.

Aturan WAJIB:
1. Gunakan Bahasa Indonesia yang hangat, langsung, dan mudah dipahami.
2. Pesan terdiri dari TEPAT 3 bagian: Prediksi Produksi | Analisis Produk | Peringatan/Catatan
3. Gunakan emoji secukupnya untuk membantu pembacaan.
4. Sesuaikan tone dengan confidence_tier:
   - green:  berikan angka spesifik + 1 rekomendasi aksi konkret
   - yellow: berikan angka dengan catatan ("ini masih perkiraan awal, terus catat ya")
   - red:    semangati saja, JANGAN beri angka atau klaim yang kuat
5. Jika ada anomaly_flags (margin turun, stockout risk), sebutkan di bagian Peringatan.
6. Jika ada missing_days > 0, ingatkan pemilik untuk mencatat di akhir pesan.
7. Tulis HANYA teks WhatsApp. Jangan tambahkan penjelasan atau komentar lain.
""".strip()


def _build_prompt(payload: dict) -> str:
    tier = payload.get("confidence_tier", "yellow")

    # Select top-3 forecast items for the message
    forecasts = [f for f in payload.get("forecasts", []) if f.get("tier") in ("green", "yellow")][:3]
    skipped   = payload.get("skipped_skus", [])
    profit    = payload.get("profit_analysis", [])[:3]
    flags     = payload.get("anomaly_flags", [])
    missing   = payload.get("consecutive_missing_days", 0)

    # Build a concise structured input — keep tokens low
    context = {
        "date":              payload.get("date"),
        "confidence_tier":   tier,
        "top_forecasts":     [
            {
                "sku_name":    f["sku_name"],
                "qty_mid":     f.get("qty_mid"),
                "qty_low":     f.get("qty_low"),
                "qty_high":    f.get("qty_high"),
                "tier":        f.get("tier"),
                "trend":       f.get("trend"),
                "stockout_risk": f.get("stockout_risk"),
            }
            for f in forecasts
        ],
        "top_profit_items":  [
            {
                "sku_name":    p["sku_name"],
                "gross_profit": p.get("gross_profit"),
                "margin_pct":  p.get("margin_pct"),
                "rank_gap":    p.get("rank_gap"),
            }
            for p in profit
        ],
        "anomaly_flags":     flags,
        "skipped_count":     len(skipped),
        "missing_days":      missing,
    }

    return (
        f"Data input:\n{json.dumps(context, ensure_ascii=False, indent=2)}\n\n"
        f"Tulis pesan WhatsApp untuk pemilik usaha berdasarkan data di atas."
    )


def generate_wa_message(payload: dict) -> str:
    """
    Call Gemini 2.0 Flash and return the WA message string.
    Falls back to a static template if the API call fails.
    """
    try:
        import google.generativeai as genai
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise EnvironmentError("GEMINI_API_KEY not set")

        model_name = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(
            model_name=model_name,
            system_instruction=_SYSTEM_PROMPT,
        )

        prompt   = _build_prompt(payload)
        response = model.generate_content(prompt)
        message  = response.text.strip()
        logger.info(f"LLM generated message ({len(message)} chars)")
        return message

    except Exception as e:
        logger.error(f"LLM error: {e} — using fallback template")
        return _fallback_message(payload)


def _fallback_message(payload: dict) -> str:
    """Static fallback when Gemini is unavailable."""
    tier  = payload.get("confidence_tier", "red")
    date_ = payload.get("date", "hari ini")

    if tier == "red":
        return (
            f"🔴 *Update Harian PIVO* — {date_}\n\n"
            "📋 *Prediksi Produksi*\n"
            "Data kamu masih sedikit. Terus catat penjualan setiap hari ya, "
            "supaya prediksinya makin akurat! 💪\n\n"
            "📊 *Analisis Produk*\n"
            "Belum bisa dianalisa minggu ini. Isi terus datanya!\n\n"
            "⚠️ *Catatan*\n"
            "Jangan lupa catat penjualan hari ini, ya!\n\n"
            "_— Dikirim otomatis oleh PIVO_"
        )
    elif tier == "yellow":
        top = payload.get("forecasts", [{}])[0]
        name = top.get("sku_name", "produk utama")
        qty  = top.get("qty_mid", "?")
        return (
            f"🟡 *Update Harian PIVO* — {date_}\n\n"
            f"📋 *Prediksi Produksi*\n"
            f"Perkiraan untuk *{name}* besok: sekitar *{qty:.0f} porsi/unit* "
            f"(masih perkiraan awal, terus catat ya!).\n\n"
            "📊 *Analisis Produk*\n"
            "Lihat dashboard PIVO untuk detail produk terlaris.\n\n"
            "⚠️ *Catatan*\n"
            "Semakin rutin kamu catat, semakin akurat prediksinya! 📝\n\n"
            "_— Dikirim otomatis oleh PIVO_"
        )
    else:
        top = payload.get("forecasts", [{}])[0]
        name = top.get("sku_name", "produk utama")
        qty  = top.get("qty_mid", "?")
        lo   = top.get("qty_low", "?")
        hi   = top.get("qty_high", "?")
        return (
            f"🟢 *Update Harian PIVO* — {date_}\n\n"
            f"📋 *Prediksi Produksi*\n"
            f"Siapkan *{name}* sebanyak *{lo:.0f}–{hi:.0f} porsi* besok "
            f"(prediksi tengah: {qty:.0f}).\n\n"
            "📊 *Analisis Produk*\n"
            "Cek produk paling menguntungkan di dashboard PIVO.\n\n"
            "✅ *Catatan*\n"
            "Hebat! Data kamu sudah cukup untuk prediksi akurat. Teruskan! 🎯\n\n"
            "_— Dikirim otomatis oleh PIVO_"
        )
