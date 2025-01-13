import React from "react";
import DocSidebar from "@theme-original/DocSidebar";
import DocsVersionDropdownNavbarItem from "@theme-original/NavbarItem/DocsVersionDropdownNavbarItem";

export default function DocSidebarWrapper(props) {
  return (
    <>
      <div className="custom-sidebarVersion">
        <p>
          <b>Version:</b> <DocsVersionDropdownNavbarItem dropdownItemsBefore={[]} dropdownItemsAfter={[]} />
        </p>
      </div>
      <hr />
      <DocSidebar {...props} />
    </>
  );
}
