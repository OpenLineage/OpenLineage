import React from "react";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";

function Footer(): JSX.Element {
  const { siteConfig } = useDocusaurusContext();
  const links = siteConfig.customFields.links.map((item, index) => (
    <ListItem data={item} key={`footer-n-l-${index}`} />
  ));
  const linksSocial = siteConfig.customFields.linksSocial.map((item, index) => (
    <ListItem data={item} key={`footer-n-l-${index}`} />
  ));

  return (
    <footer className="footer bg-bgalt py-12">
      <div className="container mx-auto text-center">
        <div className="flex justify-center my-3 mb-4">
          <a href="/" title={siteConfig?.title}>
            <img
              src={"/" + siteConfig.themeConfig.navbar.logo.src}
              alt={`${siteConfig.themeConfig.navbar.logo.alt} - logo`}
              style={{ height: "45px" }}
            />
          </a>
        </div>
        <div className="text-color-2 my-3 footer-links animated-link-parent">
          <ul>{links}</ul>
        </div>
        <div className="text-color-2 my-3 footer-links animated-link-parent">
          <ul>{linksSocial}</ul>
        </div>
        <p className="text-color-default text-lg">
          Copyright &copy; {new Date().getFullYear()} The Linux Foundation®. All rights reserved.
        </p>
      </div>
    </footer>
  );
}

function ListItem({ data }): JSX.Element {
  return (
    <li className="inline-block mx-3 animated-link-parent">
      <a href={data.to ? data.to : data.href} title={data.label} rel={data.rel ? data.rel : ""}>
        <span>{data.label}</span>
      </a>
    </li>
  );
}

export default Footer;
