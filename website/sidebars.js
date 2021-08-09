/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 * @format
 */

module.exports = {
  someSidebar: [
    'cache_library_intro',
    {
      type: 'category',
      label: 'User Guide',
      collapsed: false,
      items: [
        {
          type: 'category',
          label: 'Overview',
          collapsed: true,
          items: [
            'Cache_Library_User_Guides/doc5',
          ],
        },
      ]
    },
    {
       type: 'category',
       label: 'Architecture Guide',
       collapsed: false,
       items: [
         'Cache_Library_Architecture_Guide/doc4',
       ]
    },
    {
    "Docusaurus Styles": ['doc1','mdx'],
    }
  ],
};
