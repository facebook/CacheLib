/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 * @format
 */

import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './styles.module.css';


function LearnMore() {
  const context = useDocusaurusContext();
  const {siteConfig = {}} = context;
  return (
    <Layout
      title={`Hello from ${siteConfig.title}`}
      description="Pluggable caching engine to build and scale high performance cache services">
      <main>
	  <section>
          <div className="container">

          <div className="row">

	  <div class="col col--6">
	<p>
	    CacheLib is a general purpose caching engine that
	    facilitates the easy development, scaling and maintenance
	    of high performing caches. CacheLib was designed by
	    drawing on experiences across a range of caching use cases
	    at Facebook.  From its first deployment in 2017, CacheLib
	    now powers over 70 services at Facebook including

	    <b>Facebook’s CDN, social-graph cache, application look-aside
		cache, and block-storage system</b>.
	</p>

	<p>
	    Large scale web service providers rely on caching to
	    deliver a great user experience. While web applications
	    commonly leverage remote look-aside caches, several other
	    applications need a local in-process cache because, either
	    they can not tolerate the RPC overhead of a common remote
	    cache, or require strict cache consistency, or need domain
	    specific cache features. These application teams often
	    build and maintain application specific, highly
	    specialized, local in-process caches. Without the ability
	    to share improvements among each other, this approach
	    leads to each team solving the hard problems of scaling
	    cache performance in isolation.
	</p>

	<p>
	    CacheLib is a thread-safe, scalable, C++ library that
	    provides the core caching functionality. It enables
	    services
	    <b>
		&nbsp;to customize and scale highly concurrent caches
		easily, and to leverage the improvements across different
		caching systems by using a simple, expressive, thread-safe
		API.
	    </b>
	</p>
	  </div>

	  <div class="col col--6">
	<h4>Features</h4>
	<ul>
	    <li>
		Efficient implementations of caching indexes, eviction policies
	    </li>

	    <li>
		Support for seamless hybrid caching (caches composed
		of DRAM and Flash) to achieve high hit ratios while
		caching large working sets. Relevant for content
		delivery cache use cases.
	    </li>

	    <li>
		Optimizations for high throughput, low memory, and low
		CPU usage for a broad range of workloads.
	    </li>

	    <li>
		Native implementations of arrays and hashmaps that can
		be cached and mutated efficiently without incurring
		any serialization overhead.
	    </li>

	    <li>
		Ability to perform warm restarts that retain the state
		of the cache
	    </li>
	</ul>
	  </div>

	  </div>

	  <div className="row">
	  <div class="col col--12">
	<h2>OSDI’20</h2>


	<p>
	    <a href="https://www.usenix.org/conference/osdi20/presentation/berg">
		<em>
		    CacheLib
		</em>
		Caching Engine: Design and Experiences at Scale
	    </a>

	    was published at the USENIX OSDI 20 Conference. The
	    publication can be accessed
	    <a href="https://www.usenix.org/system/files/osdi20-berg.pdf">here</a>
	    and the video of the presentation is below.
	</p>

	<iframe width="905" height="509" src="https://www.youtube.com/embed/wp_X-Zg9WEo"
		frameborder="0"
		allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen>
	</iframe>
	  </div>
	  </div>

	  </div>
	  </section>
      </main>
    </Layout>
  );
}

export default LearnMore;
