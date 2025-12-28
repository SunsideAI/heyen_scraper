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

# OpenAI für Kurzbeschreibung
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

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
        print("[DEBUG] No existing records in Airtable to determine fields")
        return set()
    
    fields = set(all_fields[0].keys())
    print(f"[DEBUG] Existing Airtable fields: {fields}")
    return fields

def airtable_batch_create(records: List[dict]):
    """Erstelle Records in Batches"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(records), 10):
        batch = records[i:i+10]
        payload = {"records": [{"fields": r} for r in batch]}
        
        print(f"[DEBUG] Creating batch {i//10 + 1}, first record:")
        if batch:
            first_record = batch[0]
            print(f"[DEBUG]   Titel: {first_record.get('Titel', 'N/A')[:40]}")
            print(f"[DEBUG]   Preis: {first_record.get('Preis', 'MISSING')} (type: {type(first_record.get('Preis', None))})")
            print(f"[DEBUG]   Full record keys: {list(first_record.keys())}")
        
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if not r.ok:
            print(f"[DEBUG] Airtable API Error: {r.status_code}")
            print(f"[DEBUG] Response: {r.text[:500]}")
        
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
    print(f"[DEBUG] sanitize_record_for_airtable called")
    print(f"[DEBUG]   Allowed fields: {allowed_fields if allowed_fields else 'NONE (will accept all)'}")
    print(f"[DEBUG]   Record keys: {list(record.keys())}")
    print(f"[DEBUG]   Record Preis value: {record.get('Preis', 'NOT IN RECORD')}")
    
    # Felder die immer erlaubt sind (auch wenn sie in bestehenden Records leer sind)
    ALWAYS_ALLOWED = {"Kurzbeschreibung"}
    
    # Wenn keine allowed_fields gesetzt sind (z.B. erste Records), akzeptiere alles
    if not allowed_fields:
        print(f"[DEBUG]   -> Returning full record (no field restrictions)")
        return record
    
    # Kombiniere allowed_fields mit ALWAYS_ALLOWED
    all_allowed = allowed_fields | ALWAYS_ALLOWED
    
    sanitized = {k: v for k, v in record.items() if k in all_allowed}
    print(f"[DEBUG]   -> Sanitized keys: {list(sanitized.keys())}")
    print(f"[DEBUG]   -> Sanitized Preis: {sanitized.get('Preis', 'REMOVED!')}")
    
    # Check if Preis field was removed
    if "Preis" in record and "Preis" not in sanitized:
        print(f"[DEBUG]   !!! WARNING: 'Preis' field was REMOVED during sanitization!")
        print(f"[DEBUG]   !!! This means the Airtable table does not have a field named 'Preis'")
        print(f"[DEBUG]   !!! Please check your Airtable field names (case-sensitive!)")
    
    return sanitized

# ===========================================================================
# GPT KURZBESCHREIBUNG
# ===========================================================================

# Cache für existierende Kurzbeschreibungen (wird beim Start gefüllt)
KURZBESCHREIBUNG_CACHE = {}  # {objektnummer: kurzbeschreibung}

def load_kurzbeschreibung_cache():
    """Lädt existierende Kurzbeschreibungen aus Airtable in den Cache"""
    global KURZBESCHREIBUNG_CACHE
    
    if not (AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment()):
        print("[CACHE] Airtable nicht konfiguriert - Cache leer")
        return
    
    try:
        all_ids, all_fields = airtable_list_all()
        for fields in all_fields:
            obj_nr = fields.get("Objektnummer", "").strip()
            kurzbeschreibung = fields.get("Kurzbeschreibung", "").strip()
            if obj_nr and kurzbeschreibung:
                KURZBESCHREIBUNG_CACHE[obj_nr] = kurzbeschreibung
        
        print(f"[CACHE] {len(KURZBESCHREIBUNG_CACHE)} Kurzbeschreibungen aus Airtable geladen")
    except Exception as e:
        print(f"[CACHE] Fehler beim Laden: {e}")

def get_cached_kurzbeschreibung(objektnummer: str) -> str:
    """Holt Kurzbeschreibung aus Cache wenn vorhanden"""
    return KURZBESCHREIBUNG_CACHE.get(objektnummer, "")

# Einheitliche Feldstruktur für Kurzbeschreibung
KURZBESCHREIBUNG_FIELDS = [
    "Objekttyp",
    "Zimmer", 
    "Schlafzimmer",
    "Wohnfläche",
    "Grundstück",
    "Baujahr",
    "Kategorie",
    "Preis",
    "Standort",
    "Energieeffizienz",
    "Besonderheiten"
]

def normalize_kurzbeschreibung(gpt_output: str, scraped_data: dict) -> str:
    """
    Normalisiert die GPT-Ausgabe und füllt fehlende Felder mit Scrape-Daten oder '-'.
    Stellt einheitliche Struktur sicher.
    """
    # Parse GPT Output in Dictionary
    parsed = {}
    for line in gpt_output.strip().split("\n"):
        if ":" in line:
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            if value and value != "-":
                parsed[key] = value
    
    # Mapping von Scrape-Feldern zu Kurzbeschreibung-Feldern
    scrape_mapping = {
        "Zimmer": "zimmer",
        "Wohnfläche": "wohnflaeche", 
        "Grundstück": "grundstueck",
        "Baujahr": "baujahr",
        "Kategorie": "kategorie",
        "Preis": "preis",
        "Standort": "standort",
    }
    
    # Fülle fehlende Felder aus Scrape-Daten
    for field, scrape_key in scrape_mapping.items():
        if field not in parsed or not parsed[field] or parsed[field] == "-":
            scrape_value = scraped_data.get(scrape_key, "")
            if scrape_value:
                # Formatiere Preis
                if field == "Preis" and scrape_value:
                    try:
                        preis_num = float(str(scrape_value).replace(".", "").replace(",", ".").replace("€", "").strip())
                        parsed[field] = f"{int(preis_num):,} €".replace(",", ".")
                    except:
                        parsed[field] = str(scrape_value)
                # Formatiere Wohnfläche
                elif field == "Wohnfläche" and scrape_value:
                    if "m²" not in str(scrape_value):
                        parsed[field] = f"{scrape_value} m²"
                    else:
                        parsed[field] = str(scrape_value)
                # Formatiere Grundstück
                elif field == "Grundstück" and scrape_value:
                    if "m²" not in str(scrape_value):
                        parsed[field] = f"{scrape_value} m²"
                    else:
                        parsed[field] = str(scrape_value)
                else:
                    parsed[field] = str(scrape_value)
    
    # Baue einheitliche Ausgabe mit allen Feldern
    output_lines = []
    for field in KURZBESCHREIBUNG_FIELDS:
        value = parsed.get(field, "-")
        if not value or value.strip() == "":
            value = "-"
        output_lines.append(f"{field}: {value}")
    
    return "\n".join(output_lines)

def generate_kurzbeschreibung(beschreibung: str, titel: str, kategorie: str, preis: str, ort: str,
                               zimmer: str = "", wohnflaeche: str = "", grundstueck: str = "", baujahr: str = "",
                               objektnummer: str = "") -> str:
    """
    Generiert eine strukturierte Kurzbeschreibung mit GPT für die KI-Suche.
    Format ist optimiert für Regex/KI-Matching im Chatbot.
    Fehlende Felder werden aus Scrape-Daten ergänzt oder mit '-' gefüllt.
    
    OPTIMIERUNG: Wenn bereits eine Kurzbeschreibung in Airtable existiert, wird diese verwendet.
    """
    
    # CACHE CHECK: Wenn bereits vorhanden, nicht neu generieren!
    if objektnummer:
        cached = get_cached_kurzbeschreibung(objektnummer)
        if cached:
            print(f"[CACHE] Kurzbeschreibung aus Cache verwendet für {objektnummer[:30]}...")
            return cached
    
    # Scrape-Daten für Fallback sammeln
    scraped_data = {
        "kategorie": kategorie,
        "preis": preis,
        "standort": ort,
        "zimmer": zimmer,
        "wohnflaeche": wohnflaeche,
        "grundstueck": grundstueck,
        "baujahr": baujahr,
    }
    
    if not OPENAI_API_KEY:
        print("[WARN] OPENAI_API_KEY nicht gesetzt - erstelle Kurzbeschreibung aus Scrape-Daten")
        # Fallback: Erstelle Kurzbeschreibung nur aus Scrape-Daten
        return normalize_kurzbeschreibung("", scraped_data)
    
    # Baue zusätzliche Daten-Sektion für GPT
    zusatz_daten = []
    if zimmer:
        zusatz_daten.append(f"Zimmer: {zimmer}")
    if wohnflaeche:
        zusatz_daten.append(f"Wohnfläche: {wohnflaeche}")
    if grundstueck:
        zusatz_daten.append(f"Grundstück: {grundstueck}")
    if baujahr:
        zusatz_daten.append(f"Baujahr: {baujahr}")
    
    zusatz_text = "\n".join(zusatz_daten) if zusatz_daten else "Keine zusätzlichen Daten"
    
    prompt = f"""Analysiere diese Immobilienanzeige und erstelle eine strukturierte Kurzbeschreibung für eine Suchfunktion.

TITEL: {titel}
KATEGORIE: {kategorie}
PREIS: {preis if preis else 'Nicht angegeben'}
STANDORT: {ort if ort else 'Nicht angegeben'}

ZUSÄTZLICHE DATEN (aus Scraping):
{zusatz_text}

BESCHREIBUNG:
{beschreibung[:3000]}

Erstelle eine Kurzbeschreibung EXAKT in diesem Format (ALLE Felder müssen vorhanden sein, nutze "-" wenn unbekannt):

Objekttyp: [Einfamilienhaus/Mehrfamilienhaus/Eigentumswohnung/Baugrundstück/Reihenhaus/Doppelhaushälfte/Wohnung/etc. oder "-"]
Zimmer: [Anzahl oder "-"]
Schlafzimmer: [Anzahl oder "-"]
Wohnfläche: [X m² oder "-"]
Grundstück: [X m² oder "-"]
Baujahr: [Jahr oder "-"]
Kategorie: [Kaufen/Mieten]
Preis: [Preis in € oder "-"]
Standort: [PLZ Ort oder "-"]
Energieeffizienz: [Klasse A+ bis H oder "-"]
Besonderheiten: [Kommaseparierte Liste oder "-"]

WICHTIG: 
- ALLE 11 Felder MÜSSEN in der Ausgabe sein
- Nutze "-" für unbekannte/fehlende Werte
- Nutze die ZUSÄTZLICHEN DATEN wenn die Beschreibung keine Info enthält
- Zahlen ohne "ca." (z.B. "180 m²" statt "ca. 180 m²")
- Preis im Format "XXX.XXX €" """

    try:
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": "Du bist ein Experte für Immobilienanalyse. Erstelle präzise, strukturierte Kurzbeschreibungen. Halte dich EXAKT an das vorgegebene Format."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 500,
            "temperature": 0.1
        }
        
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        
        result = response.json()
        gpt_output = result["choices"][0]["message"]["content"].strip()
        
        # Normalisiere und fülle fehlende Felder
        kurzbeschreibung = normalize_kurzbeschreibung(gpt_output, scraped_data)
        
        print(f"[GPT] Kurzbeschreibung generiert und normalisiert ({len(kurzbeschreibung)} Zeichen)")
        return kurzbeschreibung
        
    except Exception as e:
        print(f"[ERROR] GPT Kurzbeschreibung fehlgeschlagen: {e}")
        # Fallback: Erstelle aus Scrape-Daten
        return normalize_kurzbeschreibung("", scraped_data)

# ===========================================================================
# EXTRACTION FUNCTIONS
# ===========================================================================

def extract_price(page_text: str) -> str:
    """Extrahiere Preis aus dem Seitentext"""
    print(f"[DEBUG] extract_price called, text length: {len(page_text)}")
    
    # Suche nach verschiedenen Preis-Patterns
    patterns = [
        # Mit Komma am Ende: "2.700.000,- €" oder "184.500,- €"
        # Flexibel für: des/der Eigentümers/Eigentümerin
        r"Kaufpreis(?:vorstellung)?[:\s]+(?:de[rs]\s+)?(?:Eigentümer(?:s|in)?[:\s]+)?€?\s*([\d.]+),?-?\s*€",
        # Standard ohne Komma: "Kaufpreis: 459.500 €"
        r"Kaufpreis(?:vorstellung)?[:\s]+(?:de[rs]\s+)?(?:Eigentümer(?:s|in)?[:\s]+)?€?\s*([\d.]+(?:,\d+)?)\s*€",
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
            # Aber nur wenn es nicht ",00" oder ähnlich ist
            preis_clean = preis_str.replace(".", "")  # Entferne Tausenderpunkte
            
            # Wenn Komma vorhanden, ersetze durch Punkt (für Dezimalstellen)
            if "," in preis_clean:
                preis_clean = preis_clean.replace(",", ".")
            
            print(f"[DEBUG] Cleaned: {preis_clean}")
            
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
        
        # Bereinige Ort - entferne häufige Zusätze
        # Pattern: alles nach " - " oder " / " oder ähnlichen Trennern
        ort = re.split(r'\s*[-–/]\s*', ort)[0].strip()
        
        # Entferne explizit "angeboten von..." Zusätze
        ort = re.sub(r'\s+(angeboten|von|der|die|das|GmbH|Immobilien).*$', '', ort, flags=re.IGNORECASE).strip()
        
        # Entferne Sonderzeichen und extra Whitespace
        ort = re.sub(r"\s+", " ", ort).strip()
        
        # Falls Ort immer noch Zusätze hat, nimm nur den ersten Teil
        if len(ort.split()) > 2:
            # Mehr als 2 Wörter -> wahrscheinlich Zusatztext
            ort = " ".join(ort.split()[:2])
        
        return f"{plz} {ort}"
    
    # Fallback: Suche nach Ortsnamen ohne PLZ im Titel
    ort_pattern = re.compile(r"\b([A-ZÄÖÜ][a-zäöüß\-]+(?:\s+[A-ZÄÖÜ][a-zäöüß\-]+)?)\b")
    for m in ort_pattern.finditer(title + " " + text[:500]):
        ort = m.group(1).strip()
        if len(ort) > 3 and ort not in ["Haus", "Wohnung", "Grundstück", "Varel", "Das", "Die", "Der"]:
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

def extract_additional_data(page_text: str) -> dict:
    """Extrahiere zusätzliche Daten für die Kurzbeschreibung"""
    data = {
        "zimmer": "",
        "wohnflaeche": "",
        "grundstueck": "",
        "baujahr": ""
    }
    
    # Zimmer extrahieren
    zimmer_patterns = [
        r"(\d+)\s*Zimmer",
        r"Zimmer[:\s]+(\d+)",
        r"(\d+)-Zimmer",
    ]
    for pattern in zimmer_patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            data["zimmer"] = m.group(1)
            break
    
    # Wohnfläche extrahieren
    wohnflaeche_patterns = [
        r"(?:ca\.\s*)?(\d+(?:[.,]\d+)?)\s*m²\s*Wohnfläche",
        r"Wohnfläche[:\s]+(?:ca\.\s*)?(\d+(?:[.,]\d+)?)\s*m²",
        r"(\d+(?:[.,]\d+)?)\s*m²\s*Wohnfl",
    ]
    for pattern in wohnflaeche_patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            data["wohnflaeche"] = m.group(1).replace(",", ".")
            break
    
    # Grundstück extrahieren
    grundstueck_patterns = [
        r"(?:ca\.\s*)?(\d+(?:[.,]\d+)?)\s*m²\s*Grundstück",
        r"Grundstück[:\s]+(?:ca\.\s*)?(\d+(?:[.,]\d+)?)\s*m²",
        r"(\d+(?:[.,]\d+)?)\s*m²\s*(?:großes?\s+)?Grundstück",
    ]
    for pattern in grundstueck_patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            data["grundstueck"] = m.group(1).replace(",", ".")
            break
    
    # Baujahr extrahieren
    baujahr_patterns = [
        r"Baujahr[:\s]+(\d{4})",
        r"aus\s+(?:dem\s+)?(?:Baujahr\s+)?(\d{4})",
        r"(\d{4})\s+(?:erbaut|gebaut)",
    ]
    for pattern in baujahr_patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            jahr = int(m.group(1))
            if 1800 <= jahr <= 2030:
                data["baujahr"] = str(jahr)
                break
    
    return data

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
    
    # Bild-URL - erstes größeres Bild (nicht das Logo)
    image_url = ""
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if src and ("/wp-content/uploads/" in src or "go-x" in src):
            # Ignoriere kleine Icons und Logos
            if any(skip in src.lower() for skip in ["logo", "icon", "favicon"]):
                continue
            
            # Ignoriere das Standard-Platzhalterbild
            if "2b42354c-5e2d-4fab-acae-4280e6ed4089" in src:
                continue
            
            # Prüfe Bildgröße anhand URL oder alt-Text
            alt = img.get("alt", "").lower()
            if "logo" in alt:
                continue
            
            image_url = src if src.startswith("http") else urljoin(BASE, src)
            print(f"[DEBUG] Found image: {image_url[:80]}...")
            break
    
    if not image_url:
        print(f"[DEBUG] No suitable image found for {detail_url}")
    
    # Kategorie
    kategorie = extract_kategorie(page_text, title, detail_url)
    
    # Objekttyp
    objekttyp = extract_objekttyp(page_text, title)
    
    # Beschreibung
    description = extract_description(soup, title, page_text)
    
    # Zusätzliche Daten extrahieren
    additional_data = extract_additional_data(page_text)
    
    # Kurzbeschreibung via GPT generieren (mit allen verfügbaren Scrape-Daten)
    # OPTIMIERUNG: Cache-Check passiert in der Funktion
    kurzbeschreibung = generate_kurzbeschreibung(
        beschreibung=description,
        titel=title,
        kategorie=kategorie,
        preis=preis,
        ort=ort,
        zimmer=additional_data["zimmer"],
        wohnflaeche=additional_data["wohnflaeche"],
        grundstueck=additional_data["grundstueck"],
        baujahr=additional_data["baujahr"],
        objektnummer=objektnummer
    )
    
    return {
        "Titel": title,
        "URL": detail_url,
        "Beschreibung": description,
        "Kurzbeschreibung": kurzbeschreibung,
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
    
    # Konvertiere Preis zu Number für Airtable
    preis_value = parse_price_to_number(row["Preis"])
    print(f"[DEBUG] make_record - Converted Preis: {preis_value} (type: {type(preis_value)})")
    
    # Wichtig: Wenn preis_value None ist, nicht ins Record aufnehmen
    # Sonst versucht Airtable ein None-Feld zu setzen
    record = {
        "Titel": row["Titel"],
        "Kategorie": row["Kategorie"],
        "Webseite": row["URL"],
        "Objektnummer": row["Objektnummer"],
        # "Objekttyp": row["Objekttyp"],  # Auskommentiert - Feld existiert nicht in Airtable
        "Beschreibung": row["Beschreibung"],
        "Bild": row["Bild_URL"],
        "Standort": row["Ort"],
    }
    
    # Kurzbeschreibung hinzufügen wenn vorhanden
    if row.get("Kurzbeschreibung"):
        record["Kurzbeschreibung"] = row["Kurzbeschreibung"]
        print(f"[DEBUG] make_record - Added Kurzbeschreibung ({len(row['Kurzbeschreibung'])} chars)")
    
    # Nur Preis hinzufügen wenn vorhanden
    if preis_value is not None:
        record["Preis"] = preis_value
        print(f"[DEBUG] make_record - Added Preis to record: {preis_value}")
    else:
        print(f"[DEBUG] make_record - WARNING: Preis is None, not adding to record")
    
    return record

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
    
    # OPTIMIERUNG: Lade existierende Kurzbeschreibungen aus Airtable
    print("[INIT] Lade Kurzbeschreibungen-Cache aus Airtable...")
    load_kurzbeschreibung_cache()
    
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
    cols = ["Titel", "Kategorie", "Webseite", "Objektnummer", "Objekttyp", "Beschreibung", "Kurzbeschreibung", "Bild", "Preis", "Standort"]
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
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
            print(f"[DEBUG] Processing record for Airtable: Titel={r.get('Titel', 'N/A')[:30]}, Preis={r.get('Preis', 'N/A')}")
            k = unique_key(r)
            if k in desired:
                if len(r.get("Beschreibung", "")) > len(desired[k].get("Beschreibung", "")):
                    desired[k] = sanitize_record_for_airtable(r, allowed)
            else:
                sanitized = sanitize_record_for_airtable(r, allowed)
                print(f"[DEBUG] After sanitization, Preis={sanitized.get('Preis', 'MISSING')}")
                desired[k] = sanitized
        
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
