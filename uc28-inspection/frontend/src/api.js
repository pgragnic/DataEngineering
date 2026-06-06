const BASE = "/api"

export const getSites = () =>
  fetch(`${BASE}/sites`).then((r) => {
    if (!r.ok) throw new Error(`HTTP ${r.status}`)
    return r.json()
  })

export const getSite = (id) =>
  fetch(`${BASE}/sites/${id}`).then((r) => {
    if (!r.ok) throw new Error(`HTTP ${r.status}`)
    return r.json()
  })

export const exportDocx = (payload) =>
  fetch(`${BASE}/rapport/docx`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  }).then((r) => {
    if (!r.ok) throw new Error(`Erreur serveur (HTTP ${r.status})`)
    return r.blob()
  })

export const synthetiser = (observation) =>
  fetch(`${BASE}/synthetiser`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ observation }),
  }).then((r) => {
    if (!r.ok) throw new Error(`Erreur serveur (HTTP ${r.status})`)
    return r.json()
  })

export const getSuggestions = (itemTexte, clause, sectionTitre) =>
  fetch(`${BASE}/suggestions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ item_texte: itemTexte, clause, section_titre: sectionTitre }),
  }).then((r) => {
    if (!r.ok) throw new Error(`Erreur serveur (HTTP ${r.status})`)
    return r.json()
  })

export const getQuestionsOuiNon = (itemTexte, clause, sectionTitre) =>
  fetch(`${BASE}/questions_oui_non`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ item_texte: itemTexte, clause, section_titre: sectionTitre }),
  }).then((r) => {
    if (!r.ok) return { questions: [] }
    return r.json()
  }).catch(() => ({ questions: [] }))

export const analyserDocumentFournisseur = (nom, contenu) =>
  fetch(`${BASE}/analyser_document_fournisseur`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ nom, contenu }),
  })
    .then((r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      return r.json()
    })
    .catch(() => ({
      resume: "Analyse indisponible — backend non joignable.",
      sections_a_risque: [],
      points_controle: [],
      nc_historique: [],
    }))

const _TRANSCRIPTION_FALLBACK = "Observation terrain — à compléter."

export const transcrireManuscrit = (imageBase64, itemTexte = "") =>
  fetch(`${BASE}/transcrire_manuscrit`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ image_base64: imageBase64, item_texte: itemTexte }),
  })
    .then((r) => {
      if (!r.ok) return { texte: _TRANSCRIPTION_FALLBACK }
      return r.json()
    })
    .catch(() => ({ texte: _TRANSCRIPTION_FALLBACK }))

export const analyser = (observation, site_id) => {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), 60000)
  return fetch(`${BASE}/analyser`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ observation, site_id: site_id || null }),
    signal: controller.signal,
  })
    .then((r) => {
      clearTimeout(timeout)
      if (!r.ok) throw new Error(`Erreur serveur (HTTP ${r.status})`)
      return r.json()
    })
    .catch((e) => {
      clearTimeout(timeout)
      if (e.name === "AbortError") throw new Error("Délai dépassé (60s) — vérifiez que le backend est démarré")
      throw e
    })
}
