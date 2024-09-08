import React, { useEffect, useRef, useState } from "react"
import Layout from '@theme/Layout'
import { Button } from "../components/ui"
import Footer from "../components/footer"
import { ArrowRight } from "react-feather"
import { Calendar } from "react-feather"
import { Slack } from "react-feather"
import { Inbox } from "react-feather"
import { GitHub } from "react-feather"

export default function Main(): JSX.Element {
  const seoTitle = 'Home';
  const seoDescription = 'Data lineage is the foundation for a new generation of powerful, context-aware data tools and best practices. OpenLineage enables consistent collection of lineage metadata, creating a deeper understanding of how data is produced and used.';

  return (
    <Layout title={seoTitle} description={seoDescription}>
      <div className="bg-bg">
        <Wall />
        <About />
        <Deploy />
        <Participate />
        <Newsletter />
        <Footer />
      </div>
    </Layout>
  );
}

const Wall = ({ twoColumnWall = false, capitalizeTitleOnHome = false }) => {
  const wall = useRef(null)
  const titleImage = 'img/background.svg';
  const [state, changeState] = useState({
    loaded: false,
    supportsBlend: false,
  })

  useEffect(() => {
    if (window.CSS && !state.loaded) {
      if (CSS.supports("mix-blend-mode", "screen")) {
        wall.current.classList.add("supports-blend")
        changeState({
          loaded: true,
          supportsBlend: true,
        })
      }
    }
  }, [state.loaded])

  let spanAttrs: Partial<{ style: unknown }> = {}

  if (!twoColumnWall && titleImage) {
    spanAttrs.style = {
      backgroundImage: `url('${titleImage}')`,
      height: '35em',
      backgroundRepeat: 'no-repeat',
      backgroundPosition: 'center',
      backgroundSize: 'cover',
    }
  }

  const innerComponents = (
    <React.Fragment>
      <div className="title">
        <h1
          className={`text-6xl relative mt-20 lg:text-7xl ${capitalizeTitleOnHome ? "uppercase" : ""
            }`}
        >
          OpenLineage
        </h1>
      </div>
      <p className="text-lg lg:text-xl text-color-3 uppercase pt-4 lg:pt-0">
        An open framework for data lineage collection and analysis
      </p>
      <p className="text-base text-color-4 boxed lg:text-lg mt-4">
        Data lineage is the foundation for a new generation of powerful, context-aware data tools and best practices. OpenLineage enables consistent collection of lineage metadata, creating a deeper understanding of how data is produced and used.
      </p>
      <span className="py-5">
        <Button
          title="Quickstart"
          to='/getting-started'
          type="link"
          iconRight={<ArrowRight />}
        />
        <Button
          title="Slack"
          to="http://bit.ly/OpenLineageSlack"
          type="link"
          iconRight={<Slack />}
        />
        <Button
          title="GitHub"
          to="https://github.com/OpenLineage"
          type="link"
          iconRight={<GitHub />}
        />
      </span>
    </React.Fragment>
  )

  if (twoColumnWall) {
    return (
      <div
        className="wall h-screen flex relative justify-center items-center overflow-hidden"
        ref={wall}
      >
        <div className="flex-1 lg:block absolute lg:relative w-full h-full top-0 left-0">
          <div
            className="absolute left-0 top-0 w-full h-full lg:hidden"
            style={{
              background: "rgba(0,0,0,.75)",
            }}
          ></div>
          <img
            src={titleImage}
            alt=""
            className="h-full w-auto max-w-none lg:h-auto lg:w-full"
          />
        </div>
        <div className="flex-1 text-center p-3 relative z-10 lg:text-left lg:pl-8 text-white lg:text-color-default">
          {innerComponents}
        </div>
      </div>
    )
  }

  return (
    <div
      className="wall flex flex-col justify-center items-center text-center mb-12"
      {...spanAttrs}
      ref={wall}
    >
      {innerComponents}
    </div>
  )
}

const About = () => {
  let spanAttrs: Partial<{ style: unknown }> = {}

  spanAttrs.style = {
    margin: '0 auto',
  }

  return (
    <div className="boxed">
      <div className="px-4 py-12 text-center lg:py-14 lg:px-0">
        <h2 className="text-color-1 text-3xl lg:text-4xl">
          About the Project
        </h2>
        <p className="mt-5 text-lg">
          OpenLineage is an open platform for collection and analysis of data lineage. It tracks metadata about datasets, jobs, and runs, giving users the information required to identify the root cause of complex issues and understand the impact of changes. OpenLineage contains an open standard for lineage data collection, a metadata repository reference implementation (Marquez), libraries for common languages, and integrations with data pipeline tools.
        </p>
        <img
          src="img/ol-stack.svg"
          alt=""
          {...spanAttrs}
          className="h-full w-4/5 max-w-none lg:h-auto lg:w-4/5 py-3 pt-6 mb-6"
        />
        <p className="mt-5 text-lg py-3">
          At the core of OpenLineage is a standard API for capturing lineage events. Pipeline components - like schedulers, warehouses, analysis tools, and SQL engines - can use this API to send data about runs, jobs, and datasets to a compatible OpenLineage backend for further study.
        </p>
        <Button
          title="Read the javadoc"
          to='/apidocs/javadoc'
          type="link"
          iconRight={<ArrowRight />}
        />
        <Button
          title="Read the openapi doc"
          to='/apidocs/openapi'
          type="link"
          iconRight={<ArrowRight />}
        />
      </div>
    </div>
  )
}

const Participate = () => {
  return (
      <div className="boxed bg-bgalt mb-24">
          <div className="px-4 py-12 text-center lg:py-14">
              <h2 className="text-color-1 text-3xl lg:text-4xl">
                  How to Participate
              </h2>
              <p className="mt-5 text-lg py-3">
                  OpenLineage is an open spec, and we welcome contributions and feedback from users and vendors alike. We have a Slack community where you can engage directly with members of the project, ask questions, and share your experiences. We also run a monthly open meeting of the Technical Steering Committee where we share project updates and engage in open discussion.
              </p>
              <Button
                  title="Slack"
                  to='https://bit.ly/OpenLineageSlack'
                  type="extbutton"
                  iconRight={<Slack />}
              />
              <Button
                  title="TSC Meetings"
                  to='/meetings'
                  type="link"
                  iconRight={<Calendar />}
              />
          </div>
      </div>
  )
}

const Newsletter = () => {
  return (
    <div className="boxed bg-bgalt">
        <div className="px-4 py-12 text-center lg:py-14">
            <h2 className="text-color-1 text-3xl lg:text-4xl">
                How to Get the Newsletter
            </h2>
            <p className="mt-5 text-lg py-3">
                Stay abreast of the latest developments in the community by subscribing to the monthly newsletter.
            </p>
            <Button
                title="Sign up"
                to='https://bit.ly/OL_news'
                type="link"
                iconRight={<Inbox />}
                className="mx-5"
            />
            <Button
                title="Archive"
                to="https://bit.ly/OL_news_archive"
                type="link"
                iconRight={<Inbox />}
                className="mx-5"
            />
        </div>
    <div className="text-center pt-8">&nbsp;</div>
      <div className="container text-center">
          <div className="row">
              <p>&nbsp;</p>
          </div>
      </div>      
    </div>
  )
}

const Deploy = () => {
  let spanAttrs: Partial<{ style: unknown }> = {}

  spanAttrs.style = {
    margin: '0 auto',
  }
  return (
    <div className="boxed bg-bgalt mb-24">
        <div className="px-4 py-12 text-center lg:py-14">
            <h2 className="text-color-1 text-3xl lg:text-4xl">
                How to Deploy OpenLineage
            </h2>
            <p className="mt-5 text-lg py-3">
                OpenLineage supports both simple deployments with single consumers and complex deployments with multiple consumers.
            </p>
            <h3 className="text-color-1 text-2xl lg:text-3xl">
                Simple
            </h3>
            <img 
              src="img/simple-deployments.png"
              alt="wireframes of simple deployments"
              {...spanAttrs}
              className="h-full w-4/5 max-w-none lg:h-auto lg:w-4/5 py-3 pt-6 mb-6"
            />
            <h3 className="text-color-1 text-2xl lg:text-3xl">
                Complex
            </h3>
            <img 
              src="img/complex-deployments.png"
              alt="wireframe of a complex deployment"
              {...spanAttrs}
              className="h-full w-4/5 max-w-none lg:h-auto lg:w-4/5 py-3 pt-6 mb-6"
            />
        </div>
      </div>
  )
}
