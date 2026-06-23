import { translations } from "./i18n"

export function useT(lang) {
  const d = translations[lang] ?? translations.FR
  return (key, subs = {}) => {
    let val = key.split(".").reduce((o, k) => o?.[k], d) ?? key
    for (const [k, v] of Object.entries(subs)) {
      val = val.replaceAll(`{${k}}`, v)
    }
    return val
  }
}
