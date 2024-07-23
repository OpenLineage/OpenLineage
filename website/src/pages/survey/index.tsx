import * as React from 'react';
import Layout from '@theme/Layout';
import Footer from "../../components/footer";

export default function Main(): JSX.Element {
    const seoTitle = 'Survey';
    const seoDescription = '';
  
    return (
      <Layout title={seoTitle} description={seoDescription}>
        <div className="mt-20 mx-20">
          <img src={require(`@site/static/survey/Section1@2x.png`).default} />
          <img src={require(`@site/static/survey/Section2@2x.png`).default} />
          <img src={require(`@site/static/survey/Section3@2x.png`).default} />
        </div>
        <div className="survey-btn-container mt-20">
          <div className="survey-btn-div">
            <a href="https://docs.google.com/forms/d/1j1SyJH0LoRNwNS1oJy0qfnDn_NPOrQw_fMb7qwouVfU/viewanalytics">
              <button type="submit" className="survey-btn">
                <span className="">View Raw Data</span>
              </button>
            </a>
          </div>
        </div>
        <div className="mb-0">
          <Footer />
        </div>
      </Layout>
    );
  }
  