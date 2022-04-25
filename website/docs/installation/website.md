---
title: CacheLib Website - Contributing to the docs
id: website
---

## CacheLib Website Overview

The `cachelib` website ( https://cachelib.org ) is hosted
as a static [github-pages](https://pages.github.com/) branch
in the [CacheLib GitHub Repository](https://github.com/facebook/cachelib/tree/gh-pages).

The static pages are auto-generated using [Docusaurus](https://docusaurus.io/)
from the markdown files in the [website sub-directory](https://github.com/facebook/CacheLib/tree/main/website)
of the `cachelib` github repository.

## Editing Documentation

export const Highlight = ({children, color}) => (
  <span
    style={{
      border: '1px solid blue',
      borderColor: color,
      color: color,
      borderRadius: '5px',
      padding: '0.2rem',
    }}>
    {children}
  </span>
);

### Using the Github Editor

To quickly make minor changes to existing pages,
click on the <Highlight color="#25c2a0">&#x1F589;  Edit this page</Highlight>
link which appears at the bottom of every documentation page.
This will open the github web editor to directly edit the corresponding markdown file,
and generate a pull-request for the change.

### Editing Markdown files locally

To edit a documentation markdown file locally,
clone the repository, edit a markdown file, then push back to github.

Example:

```sh
git clone https://github.com/facebook/CacheLib
cd CacheLib/website/docs
vi installation/website.md # Make you changes
git add installation/website.md
git commit -m "website: update documentation"
git push
```

Note: If you do not have write-permissions to the official CacheLib repository,
create a fork first, and push to your repository, then submit a pull-request.


### Adding new sections/files

When adding a new section, create the markdown file in the
relevant `website/docs/` subdirectory (e.g. `Cache_Library_User_Guides`
or `Cache_Library_Architecture_Guide`),
and add the file to the relevant sidebar category in
[website/sidebars.js](https://github.com/facebook/CacheLib/blob/main/website/sidebars.js) file.
See [docusaurus sidebars](https://docusaurus.io/docs/sidebar) to learn about
the syntax.


## Docusaurus/Markdown syntax

Docusaurus uses mostly standard Markdown syntax, with some variations.
Read about the syntax here: https://docusaurus.io/docs/markdown-features .

Unlike standard markdown, native `HTML` tags can not be used - docusaurus
treats them as MDX/JSX tags. See https://docusaurus.io/docs/markdown-features/react
for details and proper usage of custom tags. In Short:

```jsx
/* Instead of this: */
<span style="background-color: red">Foo</span>
/* Use this: */
<span style={{backgroundColor: 'red'}}>Foo</span>
```

To learn about docusaurus website strucutre in general, read
the [Docusaurus Guides](https://docusaurus.io/docs/category/guides).


## Testing Website Locally

Docusaurus requires [NodeJS](https://nodejs.org/en/download/).
The [Yarn](https://www.npmjs.com/package/yarn) package manager is also highly recommended
(alternative are possible, but examples below use `yarn`).
See [Docusaurus Installation](https://docusaurus.io/docs/installation) for details.

Start by cloning the `cachelib` repository and running `yarn` to install all
the required packages:

```sh
$ git clone https://github.com/facebook/CacheLib
Cloning into 'CacheLib'...
remote: Enumerating objects: 12809, done.
remote: Counting objects: 100% (4751/4751), done.
remote: Compressing objects: 100% (1742/1742), done.
remote: Total 12809 (delta 3580), reused 3855 (delta 2836), pack-reused 8058
Receiving objects: 100% (12809/12809), 11.80 MiB | 5.62 MiB/s, done.
Resolving deltas: 100% (6891/6891), done.

$ cd CacheLib/website/

$ yarn
yarn install v1.22.17
info No lockfile found.
[1/4] Resolving packages...
warning @docusaurus/core > webpack-dev-server > chokidar@2.1.8: Chokidar 2 does not receive security updates since 2019. Upgrade to chokidar 3 with 15x fewer dependencies

[ ... many more warnings ... ]

4/4] Building fresh packages...
success Saved lockfile.
Done in 87.82s.
```

Use `yarn run start` to build and run a dynamic version of the website
(which will also monitor markdown fiels for any changes and update the website
automatically):

```sh
$ yarn run start
yarn run v1.22.17
* docusaurus start --port 3004 --host 0.0.0.0
Starting the development server...
Docusaurus website is running at "http://localhost:3004/".

* Client
  Compiled successfully in 25.69s

: Project is running at http://0.0.0.0:8085/
: webpack output is served from /
: Content not from webpack is served from <directory>/CacheLib/website
: 404s will fallback to /index.html
```

When testing on a local computer (e.g. a laptop), `yarn run start` will also
automatically open a web browser at http://localhost:3004 for you.

If running on a remote computer, open a browser a enter the remote computer's
IP address and port 3004.

To specify explicit ports and/or hostname, use:

```sh
yarn run start --host $(hostname -f) --port 8085
```

Upon first time, `yarn run start` will build the entire website, which can take few seconds
(depending on your computer's speed).
Changes to markdown files are detected and the website will be automatically updated.


To generate a static version of the markdown files (e.g. prior to deployment, or for testing
purposes), use the following command:

```sh
$ yarn build

yarn run v1.22.17
* docusaurus build

[en] Creating an optimized production build...

* Client (100%)

* Server (100%)

Success! Generated static files in "build".

Use `npm run serve` command to test your build locally.

Done in 89.10s.
```

The `yarn build` command will take few minutes to complete. Once completed, the entire
static website is stored in the `build/` subdirectory.

The files can be served using any standard web server, or using command such as:

```sh
$ yarn run serve
yarn run v1.22.17

    Serving "build" directory at "http://localhost:3000/"
```
or

```sh
$ python3 -m http.server --bind 0.0.0.0 --directory build/ 8085
Serving HTTP on 0.0.0.0 port 8085 (http://0.0.0.0:8085/) ...
```
