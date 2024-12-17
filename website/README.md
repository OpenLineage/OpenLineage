# OpenLineage Docs

[![Covered by Argos Visual Testing](https://argos-ci.com/badge.svg)](https://app.argos-ci.com/pawel-big-lebowski/docs/reference?utm_source=OpenLineage&utm_campaign=oss)

This is a Docusaurus site, and all content can be found in `docs/`. Contributions are welcome in the form of issues or pull requests. Pages that require attention have been marked with Docusaurus Admonitions.

### New posts

We love new blog posts, and welcome content about OpenLineage! Topics include:
* experiences from users of all kinds
* supporting products and technologies
* proposals for discussion

If you are familiar with the GitHub pull request process, it is easy to propose a new blog post:

1. Fork this project.
2. Make a new directory in `/blog`. The name of the directory will become part of the posts's URL, so choose something descriptive and unique.
3. Create an `index.mdx` file in the new directory containing your blog content. Use one of the other posts as a template. The `title`, `date`, `authors`, and `description` front matter fields are all required.
4. Add your author information -- name, title, url (optional), and image_url (optional) -- to `blog/authors.yml`. 
5. Build the site locally if you want to see it in a browser and build confidence in your formatting choices.
6. Commit your changes and submit a pull request.

### New ecosystem partners for the Ecosystem page

- Add a rectangular logo in SVG format with the dimensions 300px x 150px to static/img.
- Add a record to the appropriate file and array in static/ecosystem, using the filename of the logo for the image value.

### Changes to basepages

If you want to make a change to a basepage - e.g. to add a new member to the Ecosystem page - the best way is to submit a pull request.

These basepages can be found in `src/pages`, and are formatted in markdown.

### Building openapi docs

To build the openapi docs using `redoc-cli`, run:

```
% yarn run build:docs
```

## Local development

First, clone the repo.

Install the [node version manager](https://github.com/nvm-sh/nvm) and use it to create a Node 16 environment:

```
$ nvm install 16
$ nvm use 16
```

Run Yarn to install all of the Node dependencies for the project:

```
$ yarn
```

## Local site build

You need to first build the documentation contents. This is necessary before starting the docusaurus server.

```
$ yarn build
```

This command generates static content into the `build` directory. If you want to look at it, try `cd build && python3 -m http.server`.

## Local server start

Tell Yarn to start a development server:

```
$ yarn start
```

This command provides a URL where the doc site can be viewed. Most changes are reflected live without having to restart the server.

By default, the server port will be set to 3000. In case the port is already being used, you can specify the port number when starting the server:

```
$ yarn start --port 3001
```

## Deployment

Once the site has been launched, pull requests to `main` will cause a new doc site to be shipped via GitHub Pages.

The site is deployed using the [Gatsby Publish GitHub action](https://github.com/OpenLineage/docs/blob/main/.github/workflows/deploy.yml) whenever a change is merged into `main`. 

This GitHub Action will:
* Execute `scripts/build-docs.sh`, which performs a build of the OpenAPI docs based on the latest version of the spec that has been published into `static/spec` by the [OpenLineage release script](https://github.com/OpenLineage/OpenLineage/blob/main/spec/release.sh). The resulting docs are placed into `static/apidocs/openapi`.
* Execute `yarn run build`, which performs a build of the Gatsby landing pages and places them into `public/`. The `static/` directory, containing the OpenAPI and Java client documentation, is copied into `public/` during this step.
* Replace the contents of the `gh-pages` branch of the [org domain repo](https://github.com/OpenLineage/OpenLineage.github.io) with the contents of `public/`. This will cause that repo's GitHub Action to deploy the new content.