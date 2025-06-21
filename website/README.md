# OpenLineage Website

[![Covered by Argos Visual Testing](https://argos-ci.com/badge.svg)](https://app.argos-ci.com/pawel-big-lebowski/docs/reference?utm_source=OpenLineage&utm_campaign=oss)

All content for this Docusaurus site can be found in `website/`. Contributions are welcome in the form of issues or pull requests.

### New posts

We love new blog posts and welcome content about OpenLineage! Good topics include:

* experiences from users of all kinds
* supporting products and technologies
* proposals for discussion.

If you are familiar with the GitHub pull request process, proposing a new blog post is easy:

1. Fork this project.
2. Make a new directory in `website/blog`.
3. Add your author information -- name, title, url (optional), image_url (optional) -- to `blog/authors.yml`. 
4. Create an `index.mdx` file in the new directory containing your blog. The `title`, `date`, `authors`, and `description` front matter fields are required. For the authors field, put the author name you added to `authors.yml` in an array (`[Doe]`). Please add any images to the new directory. Recommended: use one of the other posts as a template. 
5. Run the site locally to test it (recommended).
6. Commit your changes and open a pull request.

### New ecosystem partners for the Ecosystem page

1. Fork this project.
2. Add a rectangular logo in SVG format with the dimensions 300px x 150px to `static/img`.
3. Add a record to the appropriate file and array in `static/ecosystem`, using the filename of the logo for the image value, like so:

```tsx
  {
    image: "select_star_logo.png",
    org: "Select Star",
    full_name: "Select Star",
    description:
      "Select Star uses OpenLineage events to extract and generate column-level lineage, enabling precise metadata tracking, impact analysis, and comprehensive documentation of data pipelines.",
    docs_url: "https://docs.selectstar.com/",
    org_url: "https://www.selectstar.com/",
  },
```

4. Run the site locally to test it (strongly recommended).
5. Commit your changes and open a pull request.

### New meetups

1. Fork this project.
2. Add an array to `static/meetups/meetupStrings.tsx` like so:

```tsx
  {
    image: "toronto_screen.png",
    city: "Toronto",
    link: "https://www.meetup.com/openlineage/",
  },
```

3. Run the site locally to test it (recommended).
4. Commit your changes and open a pull request.

### Changes to other landing pages

If you want to make a change to one of the other landing pages -- e.g., to add a resource to the Resources page -- the best way is to submit a pull request.

These Markdown pages can be found in `src/pages`.

### Building OpenAPI docs

To build the OpenAPI docs using `redoc-cli`, run:

```shell
% yarn run build:docs
```

## Local testing

> [!IMPORTANT]
> Requires Node (>=18.0) and Yarn.

First, clone the repo and change into the `website` directory:

```shell
$ git clone git@github.com:OpenLineage/OpenLineage.git && cd website
```

Next, install the Node dependencies for the project using Yarn:

```shell
$ yarn
```

## Local site build (optional)

If desired, build the docs locally:

```shell
$ yarn build
```

This command generates static content into the `build` directory. If you want to look at it, run:

```shell
$ cd build && python3 -m http.server
```

## Local server start

Tell Yarn to start a development server:

```shell
$ yarn start
```

The server uses port 3000 by default. If the port is already allocated, you can specify a different one:

```shell
$ yarn start --port 3001
```

## Deployment

Merges to `main` trigger a new version of the site to be published at [openlineage.io](https://openlineage.io) via the [`openlineage-site`](https://github.com/OpenLineage/openlineage-site) repo using the `Build and Publish To openlineage-site repo` GitHub Actions Workflow in this repo's `.github/workflows`. Additional content is published by the [`compatibility-tests`](https://github.com/OpenLineage/compatibility-tests) repo.
