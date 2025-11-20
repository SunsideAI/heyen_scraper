#!/usr/bin/env python3
"""
Scraper für https://www.heyen-immobilien.de/kaufangebote/
Extrahiert Immobilienangebote und synct mit Airtable

Basierend auf streil-immo Scraper v1.7
"""

import os
import re
import sys
import csv
import json
import time
from urllib.parse import urljoin, urlparse
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

BASE = "https://www.heyen-immobilien.de"
# Liste von URLs zum Scrapen
LIST_URLS = [
    f"{BASE}/kaufangebote/",
    f"{BASE}/mietangebote/",
]

# Airtable
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE = os.getenv("AIRTABLE_BASE", "")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "")

# Rate Limiting
REQUEST_DELAY = 1.5

# ===========================================================================
# REGEX PATTERNS
# ===========================================================================

RE_PLZ_ORT = re.compile(r"\b(\d{5})\s+([A-ZÄÖÜ][a-zäöüß\-\s/]+)")
RE_PRICE = re.compile(r"([\d.,]+)\s*€")

# ===========================================================================
# STOPWORDS
# ===========================================================================

STOP_STRINGS = [
    "Cookie", "Datenschutz", "Impressum", "Sie haben Fragen",
    "kontakt@", "Tel:", "Fax:", "E-Mail:", "www.", "http",
    "© ", "JavaScript", "Alle Rechte", "Rufen Sie uns an",
    "Kontaktieren Sie mich", "RICHARD HEYEN", "Telefon:",
    "Mobil:", "Anschrift:"
]

# ===========================================================================
# HELPER FUNCTIONS
# ===========================================================================

def _norm(s: str) -> str:
    """Normalisiere String"""
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _clean_desc_lines(lines: List[str]) -> List[str]:
    """Bereinige Beschreibungszeilen"""
    cleaned = []
    seen = set()
    
    for line in lines:
        line = _norm(line)
        if not line or len(line) < 10:
            continue
        
        # Filtere Stopwords
        if any(stop in line for stop in STOP_STRINGS):
            continue
        
        # Dedupliziere
        line_lower = line.lower()
        if line_lower in seen:
            continue
        seen.add(line_lower)
        cleaned.append(line)
    
    return cleaned

def soup_get(url: str, delay: float = REQUEST_DELAY) -> BeautifulSoup:
    """Hole HTML und parse mit BeautifulSoup"""
    time.sleep(delay)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

# ===========================================================================
# AIRTABLE FUNCTIONS
# ===========================================================================

def airtable_table_segment() -> str:
    """Gibt base/table Segment für Airtable API zurück"""
    if not AIRTABLE_BASE or not AIRTABLE_TABLE_ID:
        return ""
    return f"{AIRTABLE_BASE}/{AIRTABLE_TABLE_ID}"

def airtable_headers() -> dict:
    """Airtable API Headers"""
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json"
    }

def airtable_list_all() -> tuple:
    """Liste alle Records aus Airtable"""
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

def airtable_existing_fields() -> set:
    """Ermittle existierende Felder"""
    _, all_fields = airtable_list_all()
    if not all_fields:
        return set()
    return set(all_fields[0].keys())

def airtable_batch_create(records: List[dict]):
    """Erstelle Records in Batches"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(records), 10):
        batch = records[i:i+10]
        payload = {"records": [{"fields": r} for r in batch]}
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def airtable_batch_update(updates: List[dict]):
    """Update Records in Batches"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(updates), 10):
        batch = updates[i:i+10]
        payload = {"records": batch}
        r = requests.patch(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def airtable_batch_delete(record_ids: List[str]):
    """Lösche Records in Batches"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(record_ids), 10):
        batch = record_ids[i:i+10]
        params = {"records[]": batch}
        r = requests.delete(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def sanitize_record_for_airtable(record: dict, allowed_fields: set) -> dict:
    """Bereinige Record für Airtable"""
    if not allowed_fields:
        return record
    return {k: v for k, v in record.items() if k in allowed_fields or not allowed_fields}

# ===========================================================================
# EXTRACTION FUNCTIONS
# ===========================================================================

def extract_price(page_text: str) -> str:
    """Extrahiere Preis aus dem Seitentext"""
    print(f"[DEBUG] extract_price called, text length: {len(page_text)}")
    
    # Suche nach verschiedenen Preis-Patterns
    patterns = [
        # Standard: "Kaufpreis: 459.500 €"
        r"[-•]?\s*Kaufpreis(?:vorstellung)?[:\s]+(?:der\s+Eigentümer[:\s]+)?€?\s*([\d.]+(?:,\d+)?)\s*€",
        # Kaltmiete
        r"[-•]?\s*Kaltmiete[:\s]+€?\s*([\d.]+(?:,\d+)?)\s*€",
        # Warmmiete
        r"[-•]?\s*Warmmiete[:\s]+€?\s*([\d.]+(?:,\d+)?)\s*€",
        # Generische Miete
        r"[-•]?\s*Miete[:\s]+€?\s*([\d.]+(?:,\d+)?)\s*€",
        # Generischer Preis
        r"[-•]?\s*Preis[:\s]+€?\s*([\d.]+(?:,\d+)?)\s*€",
    ]
    
    # Suche nach "Kaufpreis" im Text
    if "aufpreis" in page_text.lower():
        idx = page_text.lower().find("aufpreis")
        context = page_text[max(0, idx-20):min(len(page_text), idx+100)]
        print(f"[DEBUG] Found 'aufpreis' in text: ...{context}...")
    
    for i, pattern in enumerate(patterns, 1):
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            preis_str = m.group(1)
            print(f"[DEBUG] Pattern {i} matched! Extracted: {preis_str}")
            # Entferne Punkte (Tausendertrennzeichen) und ersetze Komma durch Punkt
            preis_clean = preis_str.replace(".", "").replace(",", ".")
            try:
                preis_num = float(preis_clean)
                if preis_num > 100:  # Plausibilitätsprüfung
                    result = f"€{int(preis_num):,}".replace(",", ".")
                    print(f"[DEBUG] Formatted price: {result}")
                    return result
                else:
                    print(f"[DEBUG] Price too small ({preis_num}), continuing...")
            except Exception as e:
                print(f"[DEBUG] Error converting price: {e}")
                continue
    
    print("[DEBUG] No price found!")
    return ""

def parse_price_to_number(preis_str: str) -> Optional[float]:
    """Konvertiere Preis-String zu Nummer für Airtable"""
    print(f"[DEBUG] parse_price_to_number - Input: '{preis_str}'")
    if not preis_str:
        print(f"[DEBUG] parse_price_to_number - Empty input, returning None")
        return None
    
    # Entferne Euro-Symbol und Whitespace
    clean = preis_str.replace("€", "").strip()
    
    # Deutsche Zahlenformate: 459.500 € oder 1.250,50 €
    # Entferne Punkte (Tausendertrennzeichen) und ersetze Komma durch Punkt
    clean = clean.replace(".", "").replace(",", ".")
    print(f"[DEBUG] parse_price_to_number - Cleaned: '{clean}'")
    
    try:
        result = float(clean)
        print(f"[DEBUG] parse_price_to_number - Result: {result}")
        return result
    except Exception as e:
        print(f"[DEBUG] parse_price_to_number - Error: {e}")
        return None

def extract_plz_ort(text: str, title: str = "") -> str:
    """Extrahiere PLZ und Ort aus Text"""
    # Zuerst im kompletten Text suchen
    matches = list(RE_PLZ_ORT.finditer(text))
    
    if matches:
        # Nehme erste PLZ + Ort Kombination
        m = matches[0]
        plz = m.group(1)
        ort = m.group(2).strip()
        # Bereinige Ort
        ort = re.sub(r"\s+", " ", ort).strip()
        ort = ort.split("/")[0].strip()  # Falls "Varel / Obenstrohe"
        return f"{plz} {ort}"
    
    # Fallback: Suche nach Ortsnamen ohne PLZ
    ort_pattern = re.compile(r"\b([A-ZÄÖÜ][a-zäöüß\-]+(?:\s+[A-ZÄÖÜ][a-zäöüß\-]+)?)\b")
    for m in ort_pattern.finditer(title + " " + text[:500]):
        ort = m.group(1).strip()
        if len(ort) > 3 and ort not in ["Haus", "Wohnung", "Grundstück", "Varel"]:
            return ort
    
    return ""

def extract_objektnummer(url: str) -> str:
    """Extrahiere Objektnummer aus URL"""
    # URL format: /kaufangebote/efh-in-varel-obenstrohe/
    parts = url.rstrip("/").split("/")
    if len(parts) > 0:
        slug = parts[-1]
        # Verwende den Slug als eindeutige ID
        return slug
    return ""

def extract_description(soup: BeautifulSoup, title: str, page_text: str) -> str:
    """Extrahiere strukturierte Beschreibung"""
    lines = []
    
    # Titel als erste Zeile
    if title:
        lines.append(f"=== {title.upper()} ===")
    
    # Suche nach "Die Eckdaten:" Sektion
    eckdaten_match = re.search(r"Die Eckdaten:\s*(.+?)(?=\n[A-Z][a-z]+:|$)", page_text, re.DOTALL | re.IGNORECASE)
    if eckdaten_match:
        eckdaten_text = eckdaten_match.group(1).strip()
        # Splitte in Zeilen und bereinige
        eckdaten_lines = [line.strip() for line in eckdaten_text.split("\n") if line.strip()]
        eckdaten_lines = [line.lstrip("-•").strip() for line in eckdaten_lines]
        eckdaten_lines = [line for line in eckdaten_lines if len(line) > 10]
        
        if eckdaten_lines:
            lines.append("\n=== ECKDATEN ===")
            for line in eckdaten_lines[:20]:  # Max 20 Zeilen
                lines.append(f"• {line}")
    
    # Weitere Abschnitte
    sections = [
        ("Energieausweis", r"Der Energieausweis:\s*(.+?)(?=\n[A-Z][a-z]+:|$)"),
        ("Objektbeschreibung", r"(?:Objektbeschreibung|Beschreibung):\s*(.+?)(?=\n[A-Z][a-z]+:|$)"),
    ]
    
    for section_name, pattern in sections:
        m = re.search(pattern, page_text, re.DOTALL | re.IGNORECASE)
        if m:
            section_text = m.group(1).strip()
            section_lines = [line.strip() for line in section_text.split("\n") if line.strip()]
            section_lines = [line.lstrip("-•").strip() for line in section_lines]
            section_lines = [line for line in section_lines if len(line) > 5]
            
            if section_lines:
                lines.append(f"\n=== {section_name.upper()} ===")
                lines.extend(section_lines[:15])
    
    # Bereinige finale Zeilen
    cleaned_lines = _clean_desc_lines(lines)
    
    if cleaned_lines:
        return "\n\n".join(cleaned_lines)[:12000]
    
    # Fallback: Hole alle Paragraphen
    desc_lines = []
    for p in soup.find_all("p"):
        text = _norm(p.get_text(" ", strip=True))
        if text and len(text) > 50:
            if not any(skip in text for skip in STOP_STRINGS):
                desc_lines.append(text)
    
    desc_lines = _clean_desc_lines(desc_lines)
    if desc_lines:
        return "\n\n".join(desc_lines[:10])[:12000]
    
    return ""

def extract_kategorie(page_text: str, title: str, url: str) -> str:
    """Bestimme Kategorie (Kaufen/Mieten)"""
    # Wenn URL /kaufangebote/ enthält, ist es definitiv Kaufen
    if "/kaufangebote/" in url:
        return "Kaufen"
    
    # Wenn URL /mietangebote/ enthält, ist es definitiv Mieten
    if "/mietangebote/" in url:
        return "Mieten"
    
    # Fallback: Textanalyse
    text = (title + " " + page_text).lower()
    
    # Prüfe auf explizite Miet-Keywords
    if any(keyword in text for keyword in ["zur miete", "zu vermieten", "mietangebot", "miete monatlich"]):
        return "Mieten"
    
    # Default: Kaufen
    return "Kaufen"

def extract_objekttyp(page_text: str, title: str) -> str:
    """Extrahiere Objekttyp"""
    text = title + " " + page_text
    
    objekttypen = {
        "Wohnhaus": [r"\bWohnhaus\b", r"\bEinfamilienhaus\b", r"\bEFH\b"],
        "Eigentumswohnung": [r"\bEigentumswohnung\b", r"\bWohnung\b", r"\bETW\b"],
        "Baugrundstück": [r"\bBaugrundstück\b", r"\bGrundstück\b"],
        "Wohnanlage": [r"\bWohnanlage\b", r"\bMehrfamilienhaus\b", r"\bMFH\b"],
    }
    
    for typ, patterns in objekttypen.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return typ
    
    return "Wohnhaus"  # Default

# ===========================================================================
# SCRAPING FUNCTIONS
# ===========================================================================

def collect_detail_links() -> List[str]:
    """Sammle alle Detailseiten-Links von allen Angebotsseiten"""
    all_links = []
    
    # URLs die keine echten Immobilien sind
    BLACKLIST = [
        "/finanzierung/",
        "/diskrete-kaufangebote/",
        "/diskrete-mietangebote/",
    ]
    
    for list_url in LIST_URLS:
        print(f"[LIST] Hole {list_url}")
        try:
            soup = soup_get(list_url)
            
            # Suche nach Links die zu Immobilien-Details führen
            # Format: /kaufangebote/[slug]/ oder /mietangebote/[slug]/
            for a in soup.find_all("a", href=True):
                href = a["href"]
                
                # Prüfe ob es ein Immobilien-Link ist
                if ("/kaufangebote/" in href or "/mietangebote/" in href) and href.count("/") >= 3:
                    # Ignoriere die Hauptseiten
                    if href.strip("/") in ["kaufangebote", "mietangebote"]:
                        continue
                    
                    # Ignoriere Blacklist-URLs
                    if any(blacklisted in href for blacklisted in BLACKLIST):
                        continue
                    
                    full_url = urljoin(BASE, href)
                    if full_url not in all_links and full_url not in LIST_URLS:
                        all_links.append(full_url)
        except Exception as e:
            print(f"[ERROR] Fehler beim Holen von {list_url}: {e}")
            continue
    
    print(f"[LIST] Gefunden: {len(all_links)} Immobilien gesamt")
    return all_links

def parse_detail(detail_url: str) -> dict:
    """Parse Detailseite"""
    soup = soup_get(detail_url)
    page_text = soup.get_text("\n", strip=True)
    
    # Titel - oft in H1 oder H2
    title = ""
    for tag in soup.find_all(["h1", "h2"]):
        text = _norm(tag.get_text(strip=True))
        # Ignoriere generische Titel
        if text and len(text) > 10 and text not in ["Aktuelles Kaufangebot", "Aktuelles Mietangebot"]:
            title = text
            break
    
    # Fallback 1: Nächstes H2 nach generischem H1
    if not title or title in ["Aktuelles Kaufangebot", "Aktuelles Mietangebot"]:
        h_tags = soup.find_all(["h1", "h2", "h3"])
        for i, tag in enumerate(h_tags):
            text = _norm(tag.get_text(strip=True))
            if text in ["Aktuelles Kaufangebot", "Aktuelles Mietangebot"] and i + 1 < len(h_tags):
                next_text = _norm(h_tags[i + 1].get_text(strip=True))
                if next_text and len(next_text) > 10:
                    title = next_text
                    break
    
    # Fallback 2: Suche nach Muster "Wohnhaus in..." im Text
    if not title or len(title) < 10:
        patterns = [
            r"((?:Wohnhaus|Eigentumswohnung|Baugrundstück|Wohnanlage|Apartment|Maisonette-Wohnung)\s+in\s+[A-Z][\w\s/-]+)",
            r"((?:Stilvolle|Charmante|Luxuriös|Modern)\s+\d+-Zimmer-Wohnung\s+in\s+[A-Z][\w\s/-]+)",
        ]
        for pattern in patterns:
            m = re.search(pattern, page_text)
            if m:
                title = m.group(1).strip()
                break
    
    # Objektnummer aus URL
    objektnummer = extract_objektnummer(detail_url)
    
    # Preis
    print(f"[DEBUG] Extracting price from page_text...")
    preis = extract_price(page_text)
    print(f"[DEBUG] Extracted price: '{preis}'")
    
    # PLZ/Ort
    ort = extract_plz_ort(page_text, title)
    
    # Bild-URL - erstes größeres Bild
    image_url = ""
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if src and ("/wp-content/uploads/" in src or "go-x" in src):
            # Ignoriere kleine Icons
            if "logo" not in src.lower() and "icon" not in src.lower():
                image_url = src if src.startswith("http") else urljoin(BASE, src)
                break
    
    # Kategorie
    kategorie = extract_kategorie(page_text, title, detail_url)
    
    # Objekttyp
    objekttyp = extract_objekttyp(page_text, title)
    
    # Beschreibung
    description = extract_description(soup, title, page_text)
    
    return {
        "Titel": title,
        "URL": detail_url,
        "Beschreibung": description,
        "Objektnummer": objektnummer,
        "Kategorie": kategorie,
        "Objekttyp": objekttyp,
        "Preis": preis,
        "Ort": ort,
        "Bild_URL": image_url,
    }

def make_record(row: dict) -> dict:
    """Erstelle Airtable-Record"""
    print(f"[DEBUG] make_record - Input Preis: '{row.get('Preis', 'NOT FOUND')}'")
    preis_value = parse_price_to_number(row["Preis"])
    print(f"[DEBUG] make_record - Converted Preis: {preis_value}")
    return {
        "Titel": row["Titel"],
        "Kategorie": row["Kategorie"],
        "Webseite": row["URL"],
        "Objektnummer": row["Objektnummer"],
        "Objekttyp": row["Objekttyp"],
        "Beschreibung": row["Beschreibung"],
        "Bild": row["Bild_URL"],
        "Preis": preis_value,
        "Standort": row["Ort"],
    }

def unique_key(fields: dict) -> str:
    """Eindeutiger Key für Record"""
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
    """Hauptfunktion"""
    print("[HEYEN] Starte Scraper für heyen-immobilien.de")
    
    # Sammle Links
    detail_links = collect_detail_links()
    
    if not detail_links:
        print("[WARN] Keine Links gefunden!")
        return
    
    # Scrape Details
    all_rows = []
    for i, url in enumerate(detail_links, 1):
        try:
            print(f"\n[SCRAPE] {i}/{len(detail_links)} | {url}")
            row = parse_detail(url)
            print(f"[DEBUG] Parsed row - Preis: '{row.get('Preis', 'NOT FOUND')}'")
            record = make_record(row)
            
            # Zeige Vorschau
            preis_display = record.get('Preis', 'N/A')
            print(f"  → {record['Kategorie']:8} | {record['Titel'][:60]} | {record.get('Standort', 'N/A')} | Preis: {preis_display}")
            
            all_rows.append(record)
        except Exception as e:
            print(f"[ERROR] Fehler bei {url}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    if not all_rows:
        print("[WARN] Keine Datensätze gefunden.")
        return
    
    # Speichere CSV
    csv_file = "heyen_immobilien.csv"
    cols = ["Titel", "Kategorie", "Webseite", "Objektnummer", "Objekttyp", "Beschreibung", "Bild", "Preis", "Standort"]
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(all_rows)
    print(f"\n[CSV] Gespeichert: {csv_file} ({len(all_rows)} Zeilen)")
    
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
            if k in desired:
                if len(r.get("Beschreibung", "")) > len(desired[k].get("Beschreibung", "")):
                    desired[k] = sanitize_record_for_airtable(r, allowed)
            else:
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
        
        print(f"\n[SYNC] Gesamt → create: {len(to_create)}, update: {len(to_update)}, delete: {len(to_delete_ids)}")
        
        if to_create:
            print(f"[Airtable] Erstelle {len(to_create)} neue Records...")
            airtable_batch_create(to_create)
        if to_update:
            print(f"[Airtable] Aktualisiere {len(to_update)} Records...")
            airtable_batch_update(to_update)
        if to_delete_ids:
            print(f"[Airtable] Lösche {len(to_delete_ids)} Records...")
            airtable_batch_delete(to_delete_ids)
        
        print("[Airtable] Synchronisation abgeschlossen.\n")
    else:
        print("[Airtable] ENV nicht gesetzt – Upload übersprungen.")

if __name__ == "__main__":
    run()
