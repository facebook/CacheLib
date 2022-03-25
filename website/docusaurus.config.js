/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 * @format
 */

// start-import-example
const {fbContent, fbInternalOnly} = require('internaldocs-fb-helpers');
// end-import-example

module.exports = {
  title: fbContent({
    internal: 'CacheLib (Internal)',
    external: 'CacheLib',
  }),
  tagline: 'Pluggable caching engine to build and scale high performance cache services',
  favicon: 'img/favicon.ico',

  // Settings for the production GitHub repository WITH custom domain
  // The website will be https://cachelib.org/
  // Don't forget a corrsponding CNAME file in the 'static' directory.
  url: 'https://cachelib.org',
  baseUrl: '/',
  organizationName: 'facebook', // Usually your GitHub org/user name.
  projectName: 'CacheLib', // Usually your repo name.

  onBrokenLinks: 'log',


  themeConfig: {
    announcementBar: {
      id: 'support_ukraine',
      content:
        'Support Ukraine 🇺🇦 <a target="_blank" rel="noopener noreferrer" href="https://opensource.fb.com/support-ukraine"> Help Provide Humanitarian Aid to Ukraine</a>.',
      backgroundColor: '#20232a',
      textColor: '#fff',
      isCloseable: false,
    },
    algolia:  fbContent({
      internal: undefined,
      external:{
        // If Algolia did not provide you any appId, use 'BH4D9OD16A'
        appId: 'BH4D9OD16A',

        // Public API key: it is safe to commit it
        apiKey: 'bb92084c062a63740851123e7f3f4d26',

        indexName: 'cachelib',

        // Optional: see doc section below
        contextualSearch: true,

        // Optional: Algolia search parameters
        searchParameters: {},
       },
     }),
    image: 'img/CacheLib-Logo-small.png',
    navbar: {
      title: 'CacheLib',
      logo: {
        alt: 'My Facebook Project Logo',
        src: 'img/CacheLib-Logo-small.png',
      },
	items: [
        {
          to: 'docs/installation/installation',
          activeBasePath: 'docs',
          label: 'Build and Installation',
          position: 'left',
        },
        {
          to: 'docs/',
          activeBasePath: 'docs',
          label: 'API and Usage',
          position: 'left',
        },
        {
          to: 'docs/Cache_Library_User_Guides/Cachebench_Overview',
          activeBasePath: 'docs',
          label: 'Cachebench',
          position: 'left',
        },
        {
          to: 'docs/Cache_Library_Architecture_Guide/overview_a_random_walk',
          activeBasePath: 'docs',
          label: 'Architecture Guide',
          position: 'left',
        },
        fbContent({
           internal: {
                        to: 'docs/facebook/Cache_Monitoring/Cache_Admin_Overview',
                        activeBasePath: 'docs',
                        label: 'Internals',
                        position: 'left',
                     },
           external: {
			href: '/',
                        label: '\u200C', // Zero width non-joiner unicode character
                     },
        }),
        {
          to: 'learnmore/',
          activeBasePath: 'learnmore',
          label: 'Learn More',
          position: 'right',
        },
        // Please keep GitHub link to the right for consistency.
        {
          href: 'https://github.com/facebook/CacheLib',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Reach Us',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/facebook/CacheLib',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Facebook Developer Page',
		href: 'https://www.facebook.com/cachelib/',
            },
            {
              label: 'Twitter',
              href: 'https://twitter.com/MetaOpenSource',
            },
          ],
        },
	  /*
        {
          title: 'More',
          items: [
            {
              label: 'Blog',
              to: 'blog',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/facebook/docusaurus',
            },
          ],
        },
*/
        {
          title: 'Legal',
          // Please do not remove the privacy and terms, it's a legal requirement.
          items: [
            {
              label: 'Privacy',
              href: 'https://opensource.facebook.com/legal/privacy/',
            },
            {
              label: 'Terms',
              href: 'https://opensource.facebook.com/legal/terms/',
            },
          ],
        },
      ],
      logo: {
        alt: 'Facebook Open Source Logo',
        src: 'img/oss_logo.png',
        href: 'https://opensource.facebook.com',
      },
      // Please do not remove the credits, help to publicize Docusaurus :)
      copyright: `Copyright © ${new Date().getFullYear()} Meta Platforms, Inc. Built with Docusaurus.`,
    },
  },
  presets: [
    [
      require.resolve('docusaurus-plugin-internaldocs-fb/docusaurus-preset'),
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          editUrl: fbContent({
            internal: 'https://www.internalfb.com/code/fbsource/fbcode/cachelib/public_tld/website/',
            external: 'https://github.com/facebook/CacheLib/edit/main/website/',
          }),
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        staticDocsProject: 'cachelib',
        trackingFile: 'cachelib/staticdocs/WATCHED_FILES',
        enableEditor: true,
        'remark-code-snippets': {
          baseDir: '..',
        },
      },
    ],
  ],
  customFields: {
    fbRepoName: 'fbsource',
    ossRepoPath: 'xplat/sonar',
  },
};
