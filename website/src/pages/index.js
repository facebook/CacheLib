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

import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './styles.module.css';

const features = [
  {
    title: 'Performance',
    //imageUrl: 'img/undraw_docusaurus_mountain.svg',
    description: (
      <>
           Thread safe API to build high throughput, low overhead caching services.
	    Built-in ability to transparently leverage DRAM and SSD.
      </>
    ),
  },
  {
    title: 'Scale',
    //imageUrl: 'img/undraw_docusaurus_tree.svg',
    description: (
      <>
	    Battle-tested mechanisms to provide isolation and adapt for workload changes.
	    Cache persistence to support rapid application development and deployment.
      </>
    ),
  },
  {
    title: 'Benchmarking',
    //imageUrl: 'img/undraw_docusaurus_react.svg',
    description: (
      <>
	    <a href='docs/Cache_Library_User_Guides/Cachebench_Overview'> CacheBench </a> provides a tool-kit to prototype new cache heuristics and evaluate hardware choices on industry standard cache workloads.
      </>
    ),
  },
];

function Feature({imageUrl, title, description}) {
  const imgUrl = useBaseUrl(imageUrl);
  return (
    <div className={clsx('col col--4', styles.feature)}>
      {imgUrl && (
        <div className="text--center">
          <img className={styles.featureImage} src={imgUrl} alt={title} />
        </div>
      )}
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  );
}

function Home() {
  const context = useDocusaurusContext();
  const {siteConfig = {}} = context;
  return (
    <Layout
      title={`${siteConfig.title}`}
      description="Pluggable caching engine to build and scale high performance services">
      <header className={clsx('hero hero--primary', styles.heroBanner, 'jumbotron')}>
        <div className="container">
   <div className="row">
      <div className="col col--3">
          <img className={styles.heroLogo}
               src={useBaseUrl("img/CacheLib-Logo-small.png")} />
      </div>

      <div className="col col--4 col--offset-1">
          <h1 className={clsx("hero__title", styles.clHeroTitle)}>{siteConfig.title}</h1>
          <p className={clsx("hero__subtitle", styles.clHeroTagLine)}>
            <b>Pluggable</b> caching engine
            <br/>
            to build and scale <b>high performance</b> cache services
          </p>
       </div>


     </div>


    <div className="row">
      <div className={clsx("col", "col--3", "col--offset-4", styles.buttons2)}>
            <Link
              className={clsx(
                'button button--success button--lg margin-right--lg',
                styles.getStarted,
              )}
              to={ useBaseUrl('learnmore/') }>
              Learn More
            </Link>
          </div>

      <div className={clsx("col", "col--3", styles.buttons2)}>
            <Link
              className={clsx(
                'button button--secondary button--lg',
                styles.getStarted,
              )}
              to={ useBaseUrl('docs/installation') }>
              Get Started
            </Link>
    </div>
  </div>
        </div>
      </header>
      <main>
        {features && features.length > 0 && (
          <section className={styles.features}>
            <div className="container">
              <div className="row">
                {features.map(({title, imageUrl, description}) => (
                  <Feature
                    key={title}
                    title={title}
                    imageUrl={imageUrl}
                    description={description}
                  />
                ))}
              </div>
            </div>
          </section>
        )}
	  <section>
          <div className="container">
          <div className="row">
	  <div class="col col--12" align="center">
	  <h3> Powers Facebook’s caches from Social Graph caches to Storage caches to CDNs </h3>
	  </div>
	  </div>
	  </div>
	  </section>
      </main>
    </Layout>
  );
}

export default Home;
