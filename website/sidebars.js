/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @format
 */

const {
  fbInternalOnly,
  fbContent,
} = require('docusaurus-plugin-internaldocs-fb/internal');

module.exports = {
  installationSidebar: [
    {
      type: 'category',
      label: 'Installation and Usage',
      items: [
        'installation/installation',
        'installation/testing',
        'installation/website',
        ...fbInternalOnly(['facebook/website-internal']),
        'installation/github-squash-sync',
      ],
    },
  ],

  userguideSidebar: [
    'cache_library_intro',
    ...fbInternalOnly(['facebook/Cache_Library_onboarding_questionnaire']),
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
            'Cache_Library_User_Guides/About_CacheLib',
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
            ...fbInternalOnly([
              'facebook/Cache_Monitoring/Add_monitoring_for_cache',
            ]),
            'Cache_Library_User_Guides/faq',
          ],
        },

        {
          type: 'category',
          label: 'Cache memory management',
          collapsed: true,
          items: [
            'Cache_Library_User_Guides/Item_and_Handle',
            'Cache_Library_User_Guides/eviction_policy',
            'Cache_Library_User_Guides/Partition_cache_into_pools',
            'Cache_Library_User_Guides/Configure_HashTable',
            'Cache_Library_User_Guides/Item_Destructor',
            'Cache_Library_User_Guides/Remove_callback',
            'Cache_Library_User_Guides/Cache_persistence',
            'Cache_Library_User_Guides/Cross_Host_Cache_Persistence',
            ...fbInternalOnly([
              'facebook/Cache_Persistence/Cross_Host_Persistence_APIs_Internal',
            ]),
            'Cache_Library_User_Guides/ttl_reaper',
            'Cache_Library_User_Guides/oom_protection',
            'Cache_Library_User_Guides/pool_rebalance_strategy',
            'Cache_Library_User_Guides/automatic_pool_resizing',
          ],
        },
        {
          type: 'category',
          label: 'Hybrid Cache',
          collapsed: true,
          items: [
            'Cache_Library_User_Guides/HybridCache',
            'Cache_Library_User_Guides/Configure_HybridCache',
          ],
        },
        ...fbInternalOnly([
          {
            type: 'category',
            label: 'Object Cache',
            collapsed: true,
            items: [
              'facebook/Object_Cache/Object_Cache_Decision_Guide',
              'facebook/Object_Cache/Object_Cache_User_Guide',
            ],
          },
        ]),
        {
          type: 'category',
          label: 'Advanced Features',
          collapsed: true,
          items: [
            'Cache_Library_User_Guides/chained_items',
            'Cache_Library_User_Guides/compact_cache',
            'Cache_Library_User_Guides/Structured_Cache',
          ],
        },
        {
          type: 'category',
          label: 'Reference',
          collapsed: true,
          items: [
            'Cache_Library_User_Guides/Tuning_DRAM_cache_efficiency',
            'Cache_Library_User_Guides/CacheLib_configs',
            ...fbInternalOnly(['facebook/Cache_Persistence/TW_shm_persistence_setup']),
          ],
        },
      ],
    },
  ],
  cachebenchSideBar: [
    {
      type: 'category',
      label: 'Cachebench',
      collapsed: true,
      items: [
        'Cache_Library_User_Guides/Cachebench_Overview',
        'Cache_Library_User_Guides/Configuring_cachebench_parameters',
        'Cache_Library_User_Guides/Developing_for_Cachebench',
        'Cache_Library_User_Guides/Cachebench_FB_HW_eval',
      ],
    },
  ],
  archguideSideBar: [
    {
      type: 'category',
      label: 'Architecture Guide',
      collapsed: false,
      items: [
        {
          type: 'category',
          label: 'Overview',
          collapsed: true,
          items: [
            'Cache_Library_Architecture_Guide/overview_a_random_walk',
            'Cache_Library_Architecture_Guide/common_components',
          ],
        },
        {
          type: 'category',
          label: 'RAM Cache',
          collapsed: true,
          items: [
            'Cache_Library_Architecture_Guide/ram_cache_design',
            'Cache_Library_Architecture_Guide/ram_cache_indexing_and_eviction',
            'Cache_Library_Architecture_Guide/slab_rebalancing',
            'Cache_Library_Architecture_Guide/compact_cache_design',
          ],
        },
        {
          type: 'category',
          label: 'Hybrid Cache',
          collapsed: true,
          items: [
            'Cache_Library_Architecture_Guide/hybrid_cache',
            'Cache_Library_Architecture_Guide/navy_overview',
            'Cache_Library_Architecture_Guide/small_object_cache',
            'Cache_Library_Architecture_Guide/large_object_cache',
          ],
        },
      ],
    },
  ],
  ...fbInternalOnly({
    fbInternalsSideBar: [
      {
        type: 'autogenerated',
        dirName: 'facebook/auto',
      },
      {
        type: 'category',
        label: 'Cache Monitoring (ODS, Scuba, etc.)',
        collapsed: true,
        items: [
          'facebook/Cache_Monitoring/Cache_Admin_Overview',
          'facebook/Cache_Monitoring/monitoring',
          'facebook/Cache_Monitoring/understanding_nvm_latency',
        ],
      },
      {
        type: 'category',
        label: 'Working Set Analysis',
        collapsed: true,
        items: [
          'facebook/Working_Set_Analysis/WSA_overview',
          'facebook/Working_Set_Analysis/WSA_helpful_definitions',
          'facebook/Working_Set_Analysis/WSA_analysis_and_optimizations',
          'facebook/Working_Set_Analysis/Enabling_WSA',
          'facebook/Working_Set_Analysis/WSA_logging_library',
        ],
      },
      {
        type: 'category',
        label: 'CacheLib Developers',
        collapsed: true,
        items: [
          'facebook/Cachelib_Developers/Cachelib_onboarding_guide',
          'facebook/Cachelib_Developers/How_To_Debug_A_Core',
        ],
      },
    ],
  }),
};
