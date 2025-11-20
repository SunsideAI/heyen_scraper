#!/usr/bin/env python3
"""
HEYEN Immobilien Scraper
Scrapes: https://www.heyen-immobilien.de/kaufangebote/
        https://www.heyen-immobilien.de/mietangebote/

Version: 1.0
"""

import os
import re
import sys
import csv
import json
import time
from urllib.parse import urljoin
from typing import List, Dict, Optional

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("[ERROR] Fehlende Module. Bitte installieren:")
    print("  pip install requests beautifulsoup4 lxml")
    sys.exit(1)

# ===========================================================================
# KONFIGURATION
# ===========================================================================

BASE_URL = "https://www.heyen-immobilien.de"
KAUFANGEBOTE_URL = f"{BASE_URL}/kaufangebote/"
MIETANGEBOTE_URL = f"{BASE_URL}/mietangebote/"

# Airtable
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE = os.getenv("AIRTABLE_BASE", "")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "")

# Rate Limiting
REQUEST_DELAY = 1.5

# ===========================================================================
# HELPER FUNCTIONS
# ===========================================================================

def _norm(s: str) -> str:
    """Normalisiere String"""
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()

def soup_get(url: str, delay: float = REQUEST_DELAY) -> BeautifulSoup:
    """Hole HTML und parse mit BeautifulSoup"""
    time.sleep(delay)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    print(f"  [GET] {url}")
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

# ===========================================================================
# AIRTABLE FUNCTIONS
# ===========================================================================

def airtable_table_segment() -> str:
    if not AIRTABLE_BASE or not AIRTABLE_TABLE_ID:
        return ""
    return f"{AIRTABLE_BASE}/{AIRTABLE_TABLE_ID}"

def airtable_headers() -> dict:
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json"
    }

def airtable_list_all() -> tuple:
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    all_records = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        all_records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        time.sleep(0.2)
    
    ids = [rec["id"] for rec in all_records]
    fields = [rec.get("fields", {}) for rec in all_records]
    return ids, fields

def airtable_batch_create(records: List[dict]):
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    for i in range(0, len(records), 10):
        batch = records[i:i+10]
        payload = {"records": [{"fields": r} for r in batch]}
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def airtable_batch_update(updates: List[dict]):
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    for i in range(0, len(updates), 10):
        batch = updates[i:i+10]
        payload = {"records": batch}
        r = requests.patch(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def airtable_batch_delete(record_ids: List[str]):
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    for i in range(0, len(record_ids), 10):
        batch = record_ids[i:i+10]
        params = {"records[]": batch}
        r = requests.delete(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def sanitize_record_for_airtable(record: dict, allowed_fields: set) -> dict:
    if not allowed_fields:
        return record
    return {k: v for k, v in record.items() if k in allowed_fields}

def airtable_existing_fields() -> set:
    _, all_fields = airtable_list_all()
    if not all_fields:
        return set()
    return set(all_fields[0].keys())

# ===========================================================================
# HEYEN SCRAPING
# ===========================================================================

def extract_detail_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    """Extrahiere alle Immobilien-Detail-Links"""
    links = []
    seen = set()
    
    # Suche nach Links zu Detailseiten
    # Heyen verwendet wahrscheinlich Links wie /objekt/... oder /immobilie/...
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        
        # Filtere nach typischen Detail-URLs
        if any(x in href.lower() for x in ["objekt", "detail", "expose", "immobilie"]):
            full_url = href if href.startswith("http") else urljoin(base_url, href)
            
            if full_url not in seen:
                seen.add(full_url)
                links.append(full_url)
    
    # Alternative: Suche nach Karten/Boxen mit Immobilien
    for card in soup.find_all(class_=lambda x: x and any(word in str(x).lower() for word in ["property", "immobilie", "objekt", "listing", "card"]) if x else False):
        link = card.find("a", href=True)
        if link:
            href = link.get("href", "")
            full_url = href if href.startswith("http") else urljoin(base_url, href)
            
            if full_url not in seen:
                seen.add(full_url)
                links.append(full_url)
    
    return links

def extract_all_images(soup: BeautifulSoup, detail_url: str) -> List[str]:
    """Extrahiere alle Bilder"""
    images = []
    seen = set()
    
    for img in soup.find_all("img"):
        src = img.get("src", "") or img.get("data-src", "")
        if not src or any(x in src.lower() for x in ["logo", "icon", "favicon"]):
            continue
            
        if any(ext in src.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]):
            if not src.startswith("http"):
                src = urljoin(detail_url, src)
            if src not in seen:
                seen.add(src)
                images.append(src)
    
    return images

def parse_detail_page(url: str) -> dict:
    """Parse Immobilien-Detailseite"""
    soup = soup_get(url)
    page_text = soup.get_text("\n", strip=True)
    
    # Titel
    title = ""
    for selector in ["h1", "h2", ".title", ".property-title"]:
        elem = soup.select_one(selector)
        if elem:
            title = _norm(elem.get_text())
            if len(title) > 5:
                break
    
    # Objektnummer
    objektnummer = ""
    m = re.search(r"(?:Objekt[:\s\-]*Nr|ImmoNr|Objektnummer|ID)[:\s\-]+(\S+)", page_text, re.IGNORECASE)
    if m:
        objektnummer = m.group(1).strip()
    
    # Preis
    preis = ""
    for pattern in [
        r"Kaufpreis[:\s]+€?\s*([\d.,]+)\s*€?",
        r"Kaltmiete[:\s]+€?\s*([\d.,]+)\s*€?",
        r"Preis[:\s]+€?\s*([\d.,]+)\s*€?",
    ]:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            preis_str = m.group(1).replace(".", "").replace(",", ".")
            try:
                preis_num = float(preis_str)
                if preis_num > 100:
                    preis = f"€{int(preis_num):,}".replace(",", ".")
                    break
            except:
                pass
    
    # PLZ/Ort
    ort = ""
    m = re.search(r"\b(\d{5})\s+([A-ZÄÖÜ][a-zäöüß\-\s/]+)", page_text)
    if m:
        ort = f"{m.group(1)} {_norm(m.group(2))}"
    
    # Vermarktungsart (Kaufen/Mieten)
    vermarktungsart = "Kaufen"
    if "mietangebote" in url.lower() or re.search(r"\b(zu\s+vermieten|miete|zur\s+miete|kaltmiete)\b", page_text, re.IGNORECASE):
        vermarktungsart = "Mieten"
    
    # Bilder
    all_images = extract_all_images(soup, url)
    image_url = all_images[0] if all_images else ""
    
    # Beschreibung
    description_parts = []
    for p in soup.find_all("p"):
        text = _norm(p.get_text())
        if len(text) > 50 and not any(x in text.lower() for x in ["cookie", "datenschutz", "impressum"]):
            description_parts.append(text)
            if len(description_parts) >= 5:
                break
    
    description = "\n\n".join(description_parts)
    
    # Wohnfläche
    wohnflaeche = ""
    m = re.search(r"(?:Wohnfläche|Wfl\.)[:\s]+(?:ca\.\s*)?([\d.,]+)\s*m²", page_text, re.IGNORECASE)
    if m:
        wohnflaeche = f"{m.group(1)} m²"
    
    # Zimmer
    zimmer = ""
    m = re.search(r"(\d+)\s*Zimmer", page_text, re.IGNORECASE)
    if m:
        zimmer = m.group(1)
    
    return {
        "Titel": title,
        "URL": url,
        "Beschreibung": description,
        "Objektnummer": objektnummer,
        "Kategorie": vermarktungsart,
        "Preis": preis,
        "Ort": ort,
        "Wohnfläche": wohnflaeche,
        "Zimmer": zimmer,
        "Bild_URL": image_url,
        "Alle_Bilder": ", ".join(all_images),
        "Anzahl_Bilder": len(all_images),
    }

def collect_all_properties() -> List[str]:
    """Sammle alle Immobilien-Links von Kauf- und Mietangeboten"""
    print(f"\n{'='*70}")
    print("[HEYEN] Sammle Immobilien-Links...")
    print(f"{'='*70}\n")
    
    all_links = []
    
    # 1. Kaufangebote
    print(f"[1/2] Lade Kaufangebote: {KAUFANGEBOTE_URL}")
    try:
        soup = soup_get(KAUFANGEBOTE_URL)
        links = extract_detail_links(soup, BASE_URL)
        all_links.extend(links)
        print(f"  ✓ Gefunden: {len(links)} Kaufangebote")
    except Exception as e:
        print(f"  ✗ Fehler: {e}")
    
    # 2. Mietangebote
    print(f"\n[2/2] Lade Mietangebote: {MIETANGEBOTE_URL}")
    try:
        soup = soup_get(MIETANGEBOTE_URL)
        links = extract_detail_links(soup, BASE_URL)
        all_links.extend(links)
        print(f"  ✓ Gefunden: {len(links)} Mietangebote")
    except Exception as e:
        print(f"  ✗ Fehler: {e}")
    
    # Dedupliziere
    all_links = list(dict.fromkeys(all_links))
    
    print(f"\n{'='*70}")
    print(f"[GESAMT] {len(all_links)} Immobilien gefunden")
    print(f"{'='*70}\n")
    
    return all_links

def parse_price_to_number(preis_str: str) -> Optional[float]:
    if not preis_str:
        return None
    clean = re.sub(r"[^0-9.,]", "", preis_str)
    clean = clean.replace(".", "").replace(",", ".")
    try:
        return float(clean)
    except:
        return None

def make_record(row: dict) -> dict:
    preis_value = parse_price_to_number(row["Preis"])
    return {
        "Titel": row["Titel"],
        "Kategorie": row["Kategorie"],
        "Webseite": row["URL"],
        "Objektnummer": row["Objektnummer"],
        "Beschreibung": row["Beschreibung"],
        "Bild": row["Bild_URL"],
        "Alle_Bilder": row["Alle_Bilder"],
        "Anzahl_Bilder": row["Anzahl_Bilder"],
        "Preis": preis_value,
        "Standort": row["Ort"],
        "Wohnfläche": row.get("Wohnfläche", ""),
        "Zimmer": row.get("Zimmer", ""),
    }

def unique_key(fields: dict) -> str:
    obj = (fields.get("Objektnummer") or "").strip()
    if obj:
        return f"obj:{obj}"
    url = (fields.get("Webseite") or "").strip()
    if url:
        return f"url:{url}"
    return f"hash:{hash(json.dumps(fields, sort_keys=True))}"

# ===========================================================================
# MAIN
# ===========================================================================

def run():
    print("\n" + "="*70)
    print("🏠 HEYEN IMMOBILIEN SCRAPER v1.0")
    print("="*70)
    print("Quellen:")
    print(f"  - {KAUFANGEBOTE_URL}")
    print(f"  - {MIETANGEBOTE_URL}")
    print("="*70 + "\n")
    
    # Sammle alle Links
    try:
        detail_links = collect_all_properties()
    except Exception as e:
        print(f"\n[ERROR] Fehler beim Sammeln der Links:")
        import traceback
        traceback.print_exc()
        return
    
    if not detail_links:
        print("\n[WARN] ⚠️  Keine Links gefunden!")
        print("[HINT] Prüfe ob die Website erreichbar ist")
        return
    
    # Scrape Details
    print(f"{'='*70}")
    print(f"[SCRAPING] Starte für {len(detail_links)} Immobilien")
    print(f"{'='*70}\n")
    
    all_rows = []
    for i, url in enumerate(detail_links, 1):
        try:
            print(f"[{i}/{len(detail_links)}] {url[:70]}...")
            row = parse_detail_page(url)
            record = make_record(row)
            
            print(f"  ✓ {record['Kategorie']:8} | {record['Titel'][:50]}")
            print(f"    Bilder: {record['Anzahl_Bilder']} | {record.get('Standort', 'N/A')}")
            
            all_rows.append(record)
        except Exception as e:
            print(f"  ✗ FEHLER: {e}")
            continue
    
    if not all_rows:
        print("\n[WARN] Keine Datensätze gefunden.")
        return
    
    # CSV speichern
    csv_file = "heyen_immobilien.csv"
    cols = ["Titel", "Kategorie", "Webseite", "Objektnummer", "Beschreibung", 
            "Bild", "Alle_Bilder", "Anzahl_Bilder", "Preis", "Standort", 
            "Wohnfläche", "Zimmer"]
    
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(all_rows)
    
    print(f"\n{'='*70}")
    print(f"✅ ERFOLGREICH ABGESCHLOSSEN!")
    print(f"{'='*70}")
    print(f"📄 CSV: {csv_file}")
    print(f"🏠 Immobilien: {len(all_rows)}")
    print(f"📷 Gesamt Bilder: {sum(r['Anzahl_Bilder'] for r in all_rows)}")
    
    # Statistik
    kauf = sum(1 for r in all_rows if r['Kategorie'] == 'Kaufen')
    miete = sum(1 for r in all_rows if r['Kategorie'] == 'Mieten')
    print(f"  - Kaufangebote: {kauf}")
    print(f"  - Mietangebote: {miete}")
    print(f"{'='*70}\n")
    
    # Airtable Sync
    if AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment():
        print("\n[AIRTABLE] Starte Synchronisation...")
        
        allowed = airtable_existing_fields()
        all_ids, all_fields = airtable_list_all()
        
        existing = {}
        for rec_id, f in zip(all_ids, all_fields):
            k = unique_key(f)
            existing[k] = (rec_id, f)
        
        desired = {}
        for r in all_rows:
            k = unique_key(r)
            desired[k] = sanitize_record_for_airtable(r, allowed)
        
        to_create, to_update, keep = [], [], set()
        for k, fields in desired.items():
            if k in existing:
                rec_id, old = existing[k]
                diff = {fld: val for fld, val in fields.items() if old.get(fld) != val}
                if diff:
                    to_update.append({"id": rec_id, "fields": diff})
                keep.add(k)
            else:
                to_create.append(fields)
        
        to_delete_ids = [rec_id for k, (rec_id, _) in existing.items() if k not in keep]
        
        print(f"  Create: {len(to_create)} | Update: {len(to_update)} | Delete: {len(to_delete_ids)}")
        
        if to_create:
            airtable_batch_create(to_create)
        if to_update:
            airtable_batch_update(to_update)
        if to_delete_ids:
            airtable_batch_delete(to_delete_ids)
        
        print("  ✓ Synchronisation abgeschlossen\n")

if __name__ == "__main__":
    run()
