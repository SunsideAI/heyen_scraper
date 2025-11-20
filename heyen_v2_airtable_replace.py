#!/usr/bin/env python3
"""
HEYEN Immobilien Scraper v3.0 - SELENIUM VERSION
Für: https://www.heyen-immobilien.de/kaufangebote/
     https://www.heyen-immobilien.de/mietangebote/

WARUM SELENIUM:
Die Website lädt Inhalte dynamisch per JavaScript/WordPress Page Builder
BeautifulSoup sieht nur unvollständiges HTML

INSTALLATION:
pip install selenium requests beautifulsoup4 lxml
sudo apt-get install chromium-browser chromium-chromedriver
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
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
except ImportError:
    print("[ERROR] Selenium nicht installiert: pip install selenium")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
    import requests
except ImportError:
    print("[ERROR] Module fehlen: pip install requests beautifulsoup4 lxml")
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

PAGE_LOAD_WAIT = 5

# ===========================================================================
# SELENIUM SETUP
# ===========================================================================

def create_driver():
    """Erstelle Selenium WebDriver"""
    print("[SELENIUM] Initialisiere Chrome WebDriver...")
    
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        print("  ✓ Chrome WebDriver bereit\n")
        return driver
    except Exception as e:
        print(f"  ✗ Fehler: {e}")
        print("\n[HINT] Chrome/Chromium installieren:")
        print("  sudo apt-get install chromium-browser chromium-chromedriver")
        sys.exit(1)

# ===========================================================================
# HELPER FUNCTIONS
# ===========================================================================

def _norm(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()

def is_valid_property_url(url: str) -> bool:
    """Prüft ob URL eine echte Immobilien-Detailseite ist"""
    parsed = urlparse(url)
    
    if parsed.netloc not in ["www.heyen-immobilien.de", "heyen-immobilien.de"]:
        return False
    
    path = parsed.path.lower()
    
    if not ("/kaufangebote/" in path or "/mietangebote/" in path):
        return False
    
    if path in ["/kaufangebote/", "/mietangebote/", "/kaufangebote", "/mietangebote"]:
        return False
    
    # Filtere Kategorie-Seiten
    if "diskrete-kaufangebote" in path:
        return False
    
    return True

# ===========================================================================
# AIRTABLE FUNCTIONS (gekürzt)
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
# SCRAPING MIT SELENIUM
# ===========================================================================

def collect_property_links_selenium(driver) -> List[str]:
    """Sammle alle Immobilien-Links mit Selenium"""
    print(f"\n{'='*70}")
    print("[SELENIUM] Sammle Immobilien-Links...")
    print(f"{'='*70}\n")
    
    all_links = set()
    
    # 1. Kaufangebote
    print(f"[1/2] Lade Kaufangebote: {KAUFANGEBOTE_URL}")
    try:
        driver.get(KAUFANGEBOTE_URL)
        time.sleep(PAGE_LOAD_WAIT)
        
        # Finde alle Links
        link_elements = driver.find_elements(By.TAG_NAME, "a")
        for elem in link_elements:
            try:
                href = elem.get_attribute("href")
                if href and is_valid_property_url(href):
                    all_links.add(href)
            except:
                pass
        
        print(f"  ✓ Gefunden: {len([l for l in all_links if '/kaufangebote/' in l])} Kaufangebote")
    except Exception as e:
        print(f"  ✗ Fehler: {e}")
    
    # 2. Mietangebote
    print(f"\n[2/2] Lade Mietangebote: {MIETANGEBOTE_URL}")
    try:
        driver.get(MIETANGEBOTE_URL)
        time.sleep(PAGE_LOAD_WAIT)
        
        link_elements = driver.find_elements(By.TAG_NAME, "a")
        for elem in link_elements:
            try:
                href = elem.get_attribute("href")
                if href and is_valid_property_url(href):
                    all_links.add(href)
            except:
                pass
        
        print(f"  ✓ Gefunden: {len([l for l in all_links if '/mietangebote/' in l])} Mietangebote")
    except Exception as e:
        print(f"  ✗ Fehler: {e}")
    
    all_links = list(all_links)
    
    print(f"\n{'='*70}")
    print(f"[GESAMT] {len(all_links)} Immobilien gefunden")
    print(f"{'='*70}\n")
    
    # Zeige erste 3 Links als Beispiel
    for i, link in enumerate(all_links[:3], 1):
        print(f"  {i}. {link}")
    if len(all_links) > 3:
        print(f"  ... und {len(all_links)-3} weitere")
    print()
    
    return all_links

def extract_all_images_selenium(driver) -> List[str]:
    """Extrahiere alle Bilder mit Selenium"""
    images = []
    seen = set()
    
    try:
        img_elements = driver.find_elements(By.TAG_NAME, "img")
        for img in img_elements:
            try:
                src = img.get_attribute("src") or img.get_attribute("data-src") or img.get_attribute("data-lazy-src")
                
                if not src:
                    continue
                
                # Filtere Logos/Icons
                if any(x in src.lower() for x in ["logo", "icon", "favicon", "banner"]):
                    continue
                
                # Prüfe Bildformate
                if any(ext in src.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                    if src not in seen:
                        seen.add(src)
                        images.append(src)
            except:
                pass
    except:
        pass
    
    return images

def parse_detail_page_selenium(driver, url: str) -> dict:
    """Parse Immobilien-Detailseite mit Selenium"""
    print(f"  [LOAD] {url[:70]}...")
    
    try:
        driver.get(url)
        time.sleep(PAGE_LOAD_WAIT)
        
        # Hole HTML nachdem JavaScript geladen hat
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")
        page_text = soup.get_text("\n", strip=True)
        
        # Titel - mit Selenium
        title = ""
        try:
            title_elem = driver.find_element(By.CSS_SELECTOR, "h1, .entry-title, .property-title")
            title = _norm(title_elem.text)
        except:
            # Fallback zu BeautifulSoup
            for h in soup.find_all(["h1", "h2"]):
                title = _norm(h.get_text())
                if len(title) > 5:
                    break
        
        # Objektnummer
        objektnummer = ""
        patterns = [
            r"Objekt[:\s\-]*(?:Nr|nummer)[:\s\-]*([A-Za-z0-9\-/]+)",
            r"ImmoNr[:\s\-]*([A-Za-z0-9\-/]+)",
            r"ID[:\s\-]*([A-Za-z0-9\-/]+)",
        ]
        for pattern in patterns:
            m = re.search(pattern, page_text, re.IGNORECASE)
            if m:
                objektnummer = m.group(1).strip()
                break
        
        # Preis - erweiterte Patterns
        preis = ""
        preis_patterns = [
            r"Kaufpreis[:\s]+(?:EUR\s+)?€?\s*([\d.,]+)\s*€?",
            r"Kaltmiete[:\s]+(?:EUR\s+)?€?\s*([\d.,]+)\s*€?",
            r"Preis[:\s]+(?:EUR\s+)?€?\s*([\d.,]+)\s*€?",
            r"(?:EUR|€)\s*([\d.,]+)",
        ]
        
        for pattern in preis_patterns:
            m = re.search(pattern, page_text, re.IGNORECASE)
            if m:
                preis_str = m.group(1).replace(".", "").replace(",", ".")
                try:
                    preis_num = float(preis_str)
                    if 1000 < preis_num < 10000000:  # Plausibilitätsprüfung
                        preis = f"€{int(preis_num):,}".replace(",", ".")
                        break
                except:
                    continue
        
        # PLZ/Ort
        ort = ""
        m = re.search(r"\b(\d{5})\s+([A-ZÄÖÜ][a-zäöüß\-\s/]+)", page_text)
        if m:
            ort = f"{m.group(1)} {_norm(m.group(2))}"
        
        # Vermarktungsart
        vermarktungsart = "Kaufen"
        if "/mietangebote/" in url.lower():
            vermarktungsart = "Mieten"
        
        # Wohnfläche
        wohnflaeche = ""
        m = re.search(r"(?:Wohnfläche|Wfl\.)[:\s]+(?:ca\.\s*)?([\d.,]+)\s*m²", page_text, re.IGNORECASE)
        if m:
            wohnflaeche = f"{m.group(1)} m²"
        
        # Grundstücksfläche
        grundstueck = ""
        m = re.search(r"(?:Grundstücksfläche|Grundstücksgröße|Grundstück)[:\s]+(?:ca\.\s*)?([\d.,]+)\s*m²", page_text, re.IGNORECASE)
        if m:
            grundstueck = f"{m.group(1)} m²"
        
        # Zimmer
        zimmer = ""
        m = re.search(r"(\d+(?:[,\.]\d+)?)\s*(?:-\s*)?Zimmer", page_text, re.IGNORECASE)
        if m:
            zimmer = m.group(1)
        
        # Baujahr
        baujahr = ""
        m = re.search(r"Baujahr[:\s]+(\d{4})", page_text, re.IGNORECASE)
        if m:
            baujahr = m.group(1)
        
        # Objekttyp
        objekttyp = ""
        types = {
            "einfamilienhaus": "Einfamilienhaus",
            "efh": "Einfamilienhaus",
            "doppelhaushälfte": "Doppelhaushälfte",
            "dhh": "Doppelhaushälfte",
            "reihenhaus": "Reihenhaus",
            "wohnung": "Wohnung",
            "eigentumswohnung": "Eigentumswohnung",
            "etw": "Eigentumswohnung",
            "mehrfamilienhaus": "Mehrfamilienhaus",
            "mfh": "Mehrfamilienhaus",
            "grundstück": "Grundstück",
            "baugrundstück": "Grundstück",
            "gewerbe": "Gewerbe",
        }
        
        text_lower = (title + " " + page_text).lower()
        for key, value in types.items():
            if key in text_lower:
                objekttyp = value
                break
        
        # Bilder mit Selenium
        all_images = extract_all_images_selenium(driver)
        image_url = all_images[0] if all_images else ""
        
        # Beschreibung - nur relevante Absätze
        description_parts = []
        for p in soup.find_all("p"):
            text = _norm(p.get_text())
            
            if len(text) < 30:
                continue
            
            # Filtere unwichtige Texte
            skip_words = ["cookie", "datenschutz", "impressum", "newsletter", 
                         "kontaktformular", "zustimmung", "video laden"]
            if any(x in text.lower() for x in skip_words):
                continue
            
            description_parts.append(text)
            if len(description_parts) >= 5:
                break
        
        description = "\n\n".join(description_parts)
        
        return {
            "Titel": title,
            "URL": url,
            "Beschreibung": description,
            "Objektnummer": objektnummer,
            "Kategorie": vermarktungsart,
            "Objekttyp": objekttyp,
            "Preis": preis,
            "Ort": ort,
            "Wohnfläche": wohnflaeche,
            "Grundstück": grundstueck,
            "Zimmer": zimmer,
            "Baujahr": baujahr,
            "Bild_URL": image_url,
            "Alle_Bilder": ", ".join(all_images),
            "Anzahl_Bilder": len(all_images),
        }
        
    except Exception as e:
        print(f"  ✗ Fehler: {e}")
        return None

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
    if not row:
        return None
    preis_value = parse_price_to_number(row["Preis"])
    return {
        "Titel": row["Titel"],
        "Kategorie": row["Kategorie"],
        "Objekttyp": row.get("Objekttyp", ""),
        "Webseite": row["URL"],
        "Objektnummer": row["Objektnummer"],
        "Beschreibung": row["Beschreibung"],
        "Bild": row["Bild_URL"],
        "Alle_Bilder": row["Alle_Bilder"],
        "Anzahl_Bilder": row["Anzahl_Bilder"],
        "Preis": preis_value,
        "Standort": row["Ort"],
        "Wohnfläche": row.get("Wohnfläche", ""),
        "Grundstück": row.get("Grundstück", ""),
        "Zimmer": row.get("Zimmer", ""),
        "Baujahr": row.get("Baujahr", ""),
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
    print("🏠 HEYEN IMMOBILIEN SCRAPER v3.0 - SELENIUM")
    print("="*70)
    print("Quellen:")
    print(f"  - {KAUFANGEBOTE_URL}")
    print(f"  - {MIETANGEBOTE_URL}")
    print("="*70 + "\n")
    
    # Erstelle WebDriver
    driver = create_driver()
    
    try:
        # Sammle Links
        detail_links = collect_property_links_selenium(driver)
        
        if not detail_links:
            print("\n[WARN] ⚠️  Keine Links gefunden!")
            return
        
        # Scrape Details
        print(f"{'='*70}")
        print(f"[SCRAPING] Starte für {len(detail_links)} Immobilien")
        print(f"{'='*70}\n")
        
        all_rows = []
        for i, url in enumerate(detail_links, 1):
            print(f"[{i}/{len(detail_links)}]")
            row = parse_detail_page_selenium(driver, url)
            
            if row:
                record = make_record(row)
                if record:
                    print(f"  ✓ {record['Kategorie']:8} | {record['Titel'][:45]}")
                    print(f"    {record.get('Objekttyp', 'N/A'):15} | {record.get('Standort', 'N/A')}")
                    print(f"    Bilder: {record['Anzahl_Bilder']:2} | Preis: {record.get('Preis') or 'N/A'}")
                    all_rows.append(record)
        
        if not all_rows:
            print("\n[WARN] Keine Datensätze gefunden.")
            return
        
        # CSV speichern
        csv_file = "heyen_immobilien.csv"
        cols = ["Titel", "Kategorie", "Objekttyp", "Webseite", "Objektnummer", "Beschreibung", 
                "Bild", "Alle_Bilder", "Anzahl_Bilder", "Preis", "Standort", 
                "Wohnfläche", "Grundstück", "Zimmer", "Baujahr"]
        
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(all_rows)
        
        print(f"\n{'='*70}")
        print(f"✅ ERFOLGREICH!")
        print(f"{'='*70}")
        print(f"📄 CSV: {csv_file}")
        print(f"🏠 Immobilien: {len(all_rows)}")
        print(f"📷 Gesamt Bilder: {sum(r['Anzahl_Bilder'] for r in all_rows)}")
        
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
        
    finally:
        print("[SELENIUM] Schließe WebDriver...")
        driver.quit()

if __name__ == "__main__":
    run()
