const plugin = require("tailwindcss/plugin")
const _ = require("lodash");

const gradient = plugin(function({ addUtilities, e, theme, variants }) {
    const gradients = theme("gradients", {})
    const gradientVariants = variants("gradients", [])

    const utilities = _.map(gradients, ([start, end], name) => ({
        [`.bg-gradient-${e(name)}`]: {
            backgroundImage: `linear-gradient(to right, ${start}, ${end})`,
        },
    }))

    addUtilities(utilities, gradientVariants)
})


module.exports = {
    purge: ["./src/**/*.js", "./src/**/*.jsx", "./src/**/*.ts", "./src/**/*.tsx"],
    theme: {
        gradients: theme => ({
            primary: [theme("colors.primary"), theme("colors.secondary")],
        }),
        themes: {
            dark: {
                bg: "#3e3e3e",
                bgalt: "#000",
                "color-default": "#a8a8aa",
                "color-1": "#f8f8f8",
                border: "#718096",
                primary: "#f8f8f8",
                secondary: "#fff",
                medium: "#222"
            },
        },
        colors: {
            bg: "#f8f8f8",
            bgalt: "#fff",
            "color-default": "#3e3e3e",
            "color-1": "#47576e",
            "color-2": "#74a4bc",
            "color-3": "#f26522",
            "color-4": "#3e3e3e",
            primary: "#47576e",
            secondary: "#3e3e3e",
            link: "#f26522",
            medium: "#cfd8dc",
            white: "#fff",
            black: "#000",
            transparent: "rgba(0,0,0,0)",
            error: "#f25522",
            success: "#fbb03b"
        },
        extend: {
            fontSize: {
                '7xl': '5rem'
            },
            spacing: {
                '1px': '1px',
                '2px': '2px'
            }
        },
    },
    variants: {},
    plugins: [require(`tailwind-theme-switcher`), gradient],
}


