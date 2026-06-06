import json
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "audit.db"
DATA_DIR = Path(__file__).parent.parent / "data"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS clauses_iso (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            clause   TEXT NOT NULL,
            titre    TEXT NOT NULL,
            exigence TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sites (
            site_id                    TEXT PRIMARY KEY,
            nom                        TEXT NOT NULL,
            localisation               TEXT,
            perimetre                  TEXT,
            effectif                   INTEGER,
            responsable_qualite        TEXT,
            derniere_certification_iso TEXT,
            prochaine_recertification  TEXT
        );

        CREATE TABLE IF NOT EXISTS audits_historiques (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id                   TEXT NOT NULL REFERENCES sites(site_id),
            date                      TEXT NOT NULL,
            auditeur                  TEXT,
            non_conformites_majeures  INTEGER DEFAULT 0,
            non_conformites_mineures  INTEGER DEFAULT 0,
            themes_recurrents         TEXT
        );
    """)

    if cur.execute("SELECT COUNT(*) FROM clauses_iso").fetchone()[0] == 0:
        clauses = json.loads(
            (DATA_DIR / "iso_9001_clauses.json").read_text(encoding="utf-8")
        )
        cur.executemany(
            "INSERT INTO clauses_iso (clause, titre, exigence) VALUES (:clause, :titre, :exigence)",
            clauses,
        )

    if cur.execute("SELECT COUNT(*) FROM sites").fetchone()[0] == 0:
        sites = json.loads(
            (DATA_DIR / "sites_ratp.json").read_text(encoding="utf-8")
        )
        for s in sites:
            audits = s.get("historique_audits", [])
            cur.execute(
                """INSERT INTO sites
                   (site_id, nom, localisation, perimetre, effectif,
                    responsable_qualite, derniere_certification_iso, prochaine_recertification)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    s["site_id"], s["nom"], s["localisation"], s["perimetre"],
                    s["effectif"], s["responsable_qualite"],
                    s["derniere_certification_iso"], s["prochaine_recertification"],
                ),
            )
            cur.executemany(
                """INSERT INTO audits_historiques
                   (site_id, date, auditeur,
                    non_conformites_majeures, non_conformites_mineures, themes_recurrents)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [
                    (
                        s["site_id"],
                        a["date"],
                        a["auditeur"],
                        a["non_conformites_majeures"],
                        a["non_conformites_mineures"],
                        json.dumps(a["themes_recurrents"], ensure_ascii=False),
                    )
                    for a in audits
                ],
            )

    conn.commit()
    conn.close()
