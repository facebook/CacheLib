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

const features = [
  {
    title: 'Performance',
    //imageUrl: 'img/undraw_docusaurus_mountain.svg',
    description: (
      <>
           Thread safe API to build high throughput, low overhead caching services.
	    Built-in ability to leverage DRAM and SSD caching transparently
      </>
    ),
  },
  {
    title: 'Scale',
    //imageUrl: 'img/undraw_docusaurus_tree.svg',
    description: (
      <>
	    Battle-tested mechanisms to adapt for workload changes and provide isolation.
	    Cache persistence to support quick application development and deployment.
      </>
    ),
  },
  {
    title: 'Benchmarking',
    //imageUrl: 'img/undraw_docusaurus_react.svg',
    description: (
      <>
	    Tool-kit to evaluate heuristics and hardware platforms against production workloads.
	    Platform to evaluate and measure new caching techniques.
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
      title={`Hello from ${siteConfig.title}`}
      description="Pluggable caching engine to build and scale high performance cache services">
	  <header className={clsx('hero hero--primary', styles.heroBanner, 'jumbotron')}>
        <div className="container">
          <h1 className="hero__title">{siteConfig.title}</h1>
          <p className="hero__subtitle">{siteConfig.tagline}</p>
          <div className={styles.buttons}>
            <Link
              className={clsx(
                'button button--outline button--secondary button--lg',
                styles.getStarted,
              )}
              to={ useBaseUrl('learnmore/') }>
              Learn More
            </Link>
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
	  <div class="col col--12">
	  <h1>
	  CacheLib powers Facebook’s Caches from Social Graph to Storage Caches to CDNs
          </h1>
	  </div>
	  </div>
	  </div>
	  </section>
      </main>
    </Layout>
  );
}

export default Home;
