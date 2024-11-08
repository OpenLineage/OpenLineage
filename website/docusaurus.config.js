// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const prism = require('prism-react-renderer');
const path = require('path');
const fs = require('fs');

const getCurrentVersion = () => {
    // The file is created when "docusaurus docs:version <version>" is executed.
    const versionsPath = path.resolve(__dirname, 'versions.json');
    if (fs.existsSync(versionsPath)) {
        return require(versionsPath)[0];
    } else {
        console.warn("The documentation is not versioned. Run \"docusaurus docs:version <current version>\"")
        return "<OPENLINEAGE VERSION>";
    }
}

// Replaces every occurrence of "{{PREPROCESSOR:OPENLINEAGE_VERSION}}" with the current version of the documentation.
const openLineageVersionProvider = ({filePath, fileContent}) => {
    return fileContent.replace(/{{PREPROCESSOR:OPENLINEAGE_VERSION}}/g, getCurrentVersion());
};

const links = [
    {to: '/getting-started', label: 'Getting Started', position: 'left'},
    {to: '/resources', label: 'Resources', position: 'left'},
    {to: '/ecosystem', label: 'Ecosystem', position: 'left'},
    {to: '/community', label: 'Community', position: 'left'},
    {to: '/blog', label: 'Blog', position: 'left'},
    {to: '/docs', label: 'Docs', position: 'left'},
    {to: '/survey', label: 'Ecosystem Survey 2023', position: 'left'},
]

const linksSocial = [
    {href: 'https://fosstodon.org/@openlineage', label: 'Mastodon', rel: 'me'},
    {href: 'https://twitter.com/OpenLineage', label: 'Twitter'},
    {href: 'https://www.linkedin.com/groups/13927795/', label: 'LinkedIn'},
    {href: 'http://bit.ly/OpenLineageSlack', label: 'Slack'},
    {href: 'https://github.com/OpenLineage/OpenLineage', label: 'GitHub'}
]

/** @type {import('@docusaurus/types').Config} */
const config = {
    title: 'OpenLineage',
    tagline: 'OpenLineage',
    url: 'https://openlineage.io',
    baseUrl: '/',
    onBrokenLinks: 'throw',
    onBrokenMarkdownLinks: 'throw',
    favicon: 'img/favicon.ico',
    customFields: {
        links: links,
        linksSocial: linksSocial
    },

    organizationName: 'openlineage',
    projectName: 'docs',
    i18n: {
        defaultLocale: 'en',
        locales: ['en'],
    },

    presets: [
        [
            'classic',
            /** @type {import('@docusaurus/preset-classic').Options} */
            ({
                docs: {
                    sidebarPath: require.resolve('./sidebars.js'),
                    exclude: ['**/partials/**'],
                    editUrl: ({versionDocsDirPath, docPath, version}) => {
                        if (version === 'current') {
                            return `https://github.com/OpenLineage/OpenLineage/tree/main/website/docs/${docPath}`;
                        } else {
                            return `https://github.com/OpenLineage/openlineage-site/tree/main/${versionDocsDirPath}/${docPath}`;
                        }
                    }
                },
                theme: {
                    customCss: require.resolve('./src/css/custom.css'),
                },
                blog: {
                    blogTitle: 'Blog',
                    blogDescription: 'Data lineage is the foundation for a new generation of powerful, context-aware data tools and best practices. OpenLineage enables consistent collection of lineage metadata, creating a deeper understanding of how data is produced and used.',
                    showReadingTime: true,
                    blogSidebarCount: 5,
                    blogSidebarTitle: 'Recent posts',
                    feedOptions: {
                        type: ['json'],
                        copyright: `Copyright © ${new Date().getFullYear()} The Linux Foundation®. All rights reserved.`,
                    },
                },
                pages: {
                    path: 'src/pages',
                    include: ['**/*.{js,jsx,ts,tsx,md,mdx}'],
                    exclude: [
                        'home.tsx', // this page served from plugin
                        '**/_*.{js,jsx,ts,tsx,md,mdx}',
                        '**/_*.{js,jsx,ts,tsx,md,mdx}',
                        '**/_*/**',
                        '**/*.test.{js,jsx,ts,tsx}',
                        '**/__tests__/**',
                    ],
                    mdxPageComponent: '@theme/MDXPage',
                },
                gtag: {
                    trackingID: 'G-QMTWMLMX4M',
                    anonymizeIP: true,
                },
            }),
        ],
    ],

    plugins: [
        function tailwindcssPlugin(ctx, options) {
            return {
                name: "docusaurus-tailwindcss",
                configurePostCss(postcssOptions) {
                    // Appends TailwindCSS and AutoPrefixer.
                    postcssOptions.plugins.push(require("tailwindcss"));
                    postcssOptions.plugins.push(require("autoprefixer"));
                    return postcssOptions;
                },
            };
        },
        [
            "./plugins/home-blog-plugin",
            {
                id: "blogs",
                routeBasePath: "/",
                path: "./blogs"
            },
        ],
        require.resolve('docusaurus-lunr-search')
    ],

    themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
        ({
            navbar: {
                logo: {
                    alt: 'OpenLineage',
                    src: 'img/ol-logo.svg',
                },
                items: [
                    ...links,
                    {
                        href: 'https://github.com/OpenLineage/openlineage',
                        label: 'GitHub',
                        position: 'right',
                    }
                ],
            },
            prism: {
                theme: prism.themes.github,
                darkTheme: prism.themes.dracula,
                additionalLanguages: ['java'],
            },
            colorMode: {
                defaultMode: 'light',
                disableSwitch: true,
                respectPrefersColorScheme: false,
            },
        }),

    scripts: [
        {
            src: 'https://plausible.io/js/script.js',
            defer: true,
            'data-domain': 'openlineage.io',
        },
    ],

    markdown: {
        preprocessor: openLineageVersionProvider
    }
};

module.exports = config;
