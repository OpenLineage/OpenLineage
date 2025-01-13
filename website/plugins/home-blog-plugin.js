const blogPluginExports = require("@docusaurus/plugin-content-blog");

const defaultBlogPlugin = blogPluginExports.default;

async function blogPluginExtended(...pluginArgs) {
  const blogPluginInstance = await defaultBlogPlugin(...pluginArgs);

  return {
    // Add all properties of the default blog plugin so existing functionality is preserved
    ...blogPluginInstance,
    /**
     * Override the default `contentLoaded` hook to access blog posts data
     */
    contentLoaded: async function (data) {
      // serve main page from / and /openlineage-site
      data.actions.addRoute({
        path: "/",
        exact: true,

        // The component to use for the "Home" page route
        component: "@site/src/pages/home.tsx",
      });

      data.actions.addRoute({
        path: "/openlineage-site",
        exact: true,

        // The component to use for the "Home" page route
        component: "@site/src/pages/home.tsx",
      });

      // Call the default overridden `contentLoaded` implementation
      return blogPluginInstance.contentLoaded(data);
    },
  };
}

module.exports = {
  ...blogPluginExports,
  default: blogPluginExtended,
};
