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
            'Cache_Library_User_Guides/terms',
          ],
        },
        {
          type: 'category',
          label: 'Getting Started with Cache Library',
          collapsed: true,
          items: [
            'Cache_Library_User_Guides/Set_up_a_simple_cache',
            'Cache_Library_User_Guides/Write_data_to_cache',
            'Cache_Library_User_Guides/Read_data_from_cache',
            'Cache_Library_User_Guides/Remove_data_from_cache',
            'Cache_Library_User_Guides/Visit_data_in_cache',
            'Cache_Library_User_Guides/faq',
          ],
        },

        {
          type: 'category',
          label: 'Cache memory management',
          collapsed: true,
          items: [
            'Cache_Library_User_Guides/Item_and_ItemHandle',
            'Cache_Library_User_Guides/eviction_policy',
            'Cache_Library_User_Guides/Partition_cache_into_pools',
            'Cache_Library_User_Guides/Configure_HashTable',
            'Cache_Library_User_Guides/Remove_callback',
            'Cache_Library_User_Guides/Cache_persistence',
            'Cache_Library_User_Guides/ttl_reaper',
            'Cache_Library_User_Guides/oom_protection',
            'Cache_Library_User_Guides/pool_rebalance_strategy',
            'Cache_Library_User_Guides/automatic_pool_resizing',
          ]
        },
        {
          type: 'category',
          label: 'Hybrid Cache',
          collapsed: true,
          items: [
            'Cache_Library_User_Guides/HybridCache',
            'Cache_Library_User_Guides/Configure_HybridCache',
          ]
        },
        {
          type: 'category',
          label: 'Advanced Features',
          collapsed: true,
          items: [
            'Cache_Library_User_Guides/chained_items',
            'Cache_Library_User_Guides/compact_cache',
            'Cache_Library_User_Guides/Structured_Cache',
          ]
        },
        {
          type: 'category',
          label: 'Cachebench',
          collapsed: true,
          items: [
            'Cache_Library_User_Guides/Cachebench_Overview',
            'Cache_Library_User_Guides/Developing_for_Cachebench',
            'Cache_Library_User_Guides/Configuring_cachebench_parameters',
          ]
        },
        {
          type: 'category',
          label: 'Reference',
          collapsed: true,
          items: [
            'Cache_Library_User_Guides/Tuning_DRAM_cache_efficiency',
            'Cache_Library_User_Guides/CacheLib_configs'
          ]
        }
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
