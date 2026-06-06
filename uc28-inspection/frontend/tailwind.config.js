/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx}"],
  theme: {
    extend: {
      colors: {
        // UC28 Design System — tokens sémantiques (pointent vers les variables CSS de index.css)
        // Couleurs avec support opacité : rgb(var(--*-rgb) / <alpha-value>) permet bg-brand/10 etc.
        'brand':          'rgb(var(--brand-rgb) / <alpha-value>)',
        'brand-cyan':     'rgb(var(--brand-cyan-rgb) / <alpha-value>)',
        'brand-emerald':  'rgb(var(--emerald-rgb) / <alpha-value>)',
        'brand-amber':    'rgb(var(--amber-rgb) / <alpha-value>)',
        'nc-majeure':     'rgb(var(--nc-majeure-rgb) / <alpha-value>)',
        'nc-mineure':     'rgb(var(--nc-mineure-rgb) / <alpha-value>)',
        'observation':    'rgb(var(--observation-rgb) / <alpha-value>)',
        'conforme':       'rgb(var(--conforme-rgb) / <alpha-value>)',
        // Neutres et fonds (pas d'opacité nécessaire)
        'dark-teal':      'var(--dark-teal)',
        'canvas':         'var(--bg)',
        'surface':        'var(--surface)',
        'surface-sunk':   'var(--surface-sunk)',
        'divider':        'var(--border)',
        'ink':            'var(--text)',
        'ink-muted':      'var(--text-muted)',
        // Washes
        'wash-cyan':      'var(--wash-cyan)',
        'wash-emerald':   'var(--wash-emerald)',
      },
      boxShadow: {
        sm:     'var(--shadow-sm)',
        md:     'var(--shadow-md)',
        lg:     'var(--shadow-lg)',
        inset:  'var(--shadow-inset)',
      },
    },
  },
  plugins: [],
}

