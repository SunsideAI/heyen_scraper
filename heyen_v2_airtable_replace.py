#!/usr/bin/env python3
"""
Test-Script um zu prüfen ob Preis-Feld in Airtable funktioniert
"""
import os
import requests
import time

# Airtable Config
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE = os.getenv("AIRTABLE_BASE", "")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "")

if not AIRTABLE_TOKEN or not AIRTABLE_BASE or not AIRTABLE_TABLE_ID:
    print("❌ Bitte setze AIRTABLE_TOKEN, AIRTABLE_BASE und AIRTABLE_TABLE_ID")
    exit(1)

url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE_ID}"
headers = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

print("=" * 60)
print("AIRTABLE PREIS-FELD TEST")
print("=" * 60)

# Test 1: Liste alle existierenden Records
print("\n1️⃣ Hole existierende Records...")
r = requests.get(url, headers=headers, params={"maxRecords": 1}, timeout=30)
if r.ok:
    data = r.json()
    if data.get("records"):
        fields = data["records"][0].get("fields", {})
        print(f"✅ Existierende Felder: {list(fields.keys())}")
        print(f"   Preis vorhanden: {'Preis' in fields}")
        if 'Preis' in fields:
            print(f"   Preis Wert: {fields['Preis']} (Typ: {type(fields['Preis'])})")
    else:
        print("⚠️  Keine Records vorhanden")
else:
    print(f"❌ Fehler: {r.status_code} - {r.text}")

# Test 2: Erstelle Test-Record MIT Preis
print("\n2️⃣ Erstelle Test-Record MIT Preis-Feld...")
test_record = {
    "records": [
        {
            "fields": {
                "Titel": "🧪 TEST - Bitte löschen",
                "Kategorie": "Kaufen",
                "Preis": 999999.0,
                "Standort": "Test"
            }
        }
    ]
}

print(f"Sende: {test_record}")
r = requests.post(url, headers=headers, json=test_record, timeout=30)

if r.ok:
    print(f"✅ Record erstellt!")
    created = r.json()
    record_id = created["records"][0]["id"]
    fields_sent = test_record["records"][0]["fields"]
    fields_returned = created["records"][0].get("fields", {})
    
    print(f"\n📤 Gesendet:")
    print(f"   Preis: {fields_sent.get('Preis')} (Typ: {type(fields_sent.get('Preis'))})")
    
    print(f"\n📥 Empfangen:")
    print(f"   Felder: {list(fields_returned.keys())}")
    if 'Preis' in fields_returned:
        print(f"   ✅ Preis: {fields_returned['Preis']} (Typ: {type(fields_returned['Preis'])})")
    else:
        print(f"   ❌ Preis fehlt in Response!")
        print(f"   Verfügbare Felder: {fields_returned}")
    
    # Lösche Test-Record
    print(f"\n🗑️  Lösche Test-Record...")
    time.sleep(1)
    r = requests.delete(f"{url}/{record_id}", headers=headers, timeout=30)
    if r.ok:
        print(f"✅ Test-Record gelöscht")
    else:
        print(f"⚠️  Konnte nicht löschen: {r.status_code}")
else:
    print(f"❌ Fehler beim Erstellen: {r.status_code}")
    print(f"Response: {r.text}")
    
    # Prüfe ob Fehler wegen Feld-Typ
    if "INVALID_VALUE_FOR_COLUMN" in r.text or "Unknown field name" in r.text:
        print("\n⚠️  FEHLER-ANALYSE:")
        print("   Das Preis-Feld existiert möglicherweise nicht oder hat den falschen Typ!")
        print("   Bitte prüfe in Airtable:")
        print("   1. Feld heißt EXAKT 'Preis' (case-sensitive)")
        print("   2. Feld-Typ ist 'Number' (nicht 'Text' oder 'Currency')")

print("\n" + "=" * 60)
