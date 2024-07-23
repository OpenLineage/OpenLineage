import React from "react"
import { compileSync } from '@mdx-js/mdx'

const Link = props => {
    if (props.to) {
        return <a href={props.to} className="btn btn-primary mx-5">{props.children}</a>
    } else {
        return (
            <button {...props}></button>
        )
    }
}

const getPageData = file => {
    const compiled = compileSync(file);
    return compiled.frontMatter
}

const getDate = (date) => new Date(date).toLocaleDateString()

export { Link, getPageData, getDate }
