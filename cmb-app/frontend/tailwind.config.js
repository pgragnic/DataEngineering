/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        cmb: {
          red: '#C8102E',
          'red-dark': '#A00D24',
          'red-light': '#F5C6CE',
          gray: '#F4F6F8',
          'gray-mid': '#6B7280',
          'gray-dark': '#1F2937',
        }
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
      }
    },
  },
  plugins: [],
}
