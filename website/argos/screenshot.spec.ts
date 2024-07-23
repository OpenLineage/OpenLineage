import * as fs from "fs";
import {test} from "@playwright/test";
import {argosScreenshot} from "@argos-ci/playwright";
import {extractSitemapPathnames, pathnameToArgosName} from "argos/utils";

// Constants:
const siteUrl = "http://localhost:3000";
const sitemapPath = "../build/sitemap.xml";
const stylesheetPath = "./screenshot.css";
const stylesheet = fs.readFileSync(stylesheetPath).toString();

// Wait for hydration, requires Docusaurus v2.4.3+
// See https://github.com/facebook/docusaurus/pull/9256
// Docusaurus adds a <html data-has-hydrated="true"> once hydrated
function waitForDocusaurusHydration() {
    // uncomment the line when Docusaurus is upgraded to v2.4.3
    // return document.documentElement.dataset.hasHydrated === "true";
    return true;
}

function screenshotPathname(pathname: string, index: number, numberOfPaths: number) {
    test(`pathname ${pathname}`, async ({page}) => {
        const url = siteUrl + pathname;
        console.log(`${index + 1}/${numberOfPaths} Screenshotting`, url);
        await page.goto(url);
        await page.waitForFunction(waitForDocusaurusHydration);
        await page.addStyleTag({content: stylesheet});
        await argosScreenshot(page, pathnameToArgosName(pathname));
    });
}

test.describe("Docusaurus site screenshots", () => {
    const pathnames = extractSitemapPathnames(sitemapPath);
    console.log("Pathnames to screenshot:", pathnames);
    pathnames.forEach((path, index) => screenshotPathname(path, index, pathnames.length));
});