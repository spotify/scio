/*!
 Paradox Material Theme
 Copyright (c) 2017 Jonas Fonseca
 License: MIT
*/

function initParadoxMaterialTheme() {
  // callout -> ammonition
  document.querySelectorAll('.callout').forEach(callout => {
    callout.classList.add('admonition')
    callout.querySelectorAll('.callout-title').forEach(title => {
      title.classList.add('admonition-title')
    })
    callout.style.visibility = 'visible';
  })

  var headers = ['h2', 'h3', 'h4', 'h5', 'h6']
  headers.forEach(headerName => {
    document.querySelectorAll(headerName).forEach(header => {
      var link = header.querySelector('a')
      if (link) {
        header.id = link.name
        link.name = ''
        header.removeChild(link)
        link.text = '¶'
        link.title = 'Permanent link'
        link.className = 'headerlink'
        header.appendChild(link)
      }
    })
  })

  document.querySelectorAll('nav.md-nav--primary > ul').forEach((root, rootIndex) => {
    function createNavToggle(path, active) {
      var input = document.createElement('input')
      input.classList.add('md-toggle')
      input.classList.add('md-nav__toggle')
      input.type = 'checkbox'
      input.id = path
      input.checked = active || false
      input.setAttribute('data-md-toggle', path)
      return input
    }

    function createNavLabel(path, active, contentNode) {
      var label = document.createElement('label')
      label.classList.add('md-nav__link')
      if (active)
        label.classList.add('md-nav__link--active')
      label.setAttribute('for', path)
      if (contentNode)
        label.appendChild(contentNode)
      return label
    }

    function visitListItem(item, path, level) {
      item.classList.add('md-nav__item')

      var link = item.querySelector(':scope > a')
      if (link) {
        link.classList.add('md-nav__link')
        link.classList.remove('page')
        if (link.classList.contains('active')) {
          item.classList.add('md-nav__item--active')
          link.classList.add('md-nav__link--active')
        }
        link.setAttribute('data-md-state', '')
      }

      var nestedNav = null
      var nestedRoot = item.querySelector(':scope > ul')
      if (nestedRoot) {
        var active = item.querySelector(':scope a.active') != null
        item.classList.add('md-nav__item--nested')
        var nestedNav = document.createElement('nav')
        nestedNav.classList.add('md-nav')
        nestedNav.setAttribute('data-md-component', 'collapsible')
        nestedNav.setAttribute('data-md-level', level)

        var input = createNavToggle(path, active)

        var label = createNavLabel(path, false, link)
        if (link)
          link.classList.remove('md-nav__link')

        var labelInner = document.createElement('label')
        labelInner.classList.add('md-nav__title')
        labelInner.setAttribute('for', path)
        labelInner.textContent = link ? link.textContent : '???'

        nestedNav.appendChild(labelInner)
        nestedNav.appendChild(nestedRoot)
        item.appendChild(input)
        item.appendChild(label)
        item.appendChild(nestedNav)
        visitList(nestedRoot, path, level + 1)
      }

      if (link && link.classList.contains('active')) {
        var toc = document.querySelector('nav.md-nav--primary > .md-nav--secondary')
        if (toc && toc.children.length > 0) {
          var input = createNavToggle('__toc', false)
          var labelText = nestedNav ? 'Table of contents' : link ? link.textContent : '???'
          var label = createNavLabel('__toc', true, document.createTextNode(labelText))

          if (nestedNav) {
            var node = nestedNav.children[1]
            nestedNav.insertBefore(input, node)
            nestedNav.insertBefore(label, node)
            nestedNav.appendChild(toc)
          } else if (link) {
            item.insertBefore(input, link)
            item.insertBefore(label, link)
            item.appendChild(toc)
          }
        }
      }
    }

    function visitList(list, path, level) {
      list.classList.add('md-nav__list')
      list.setAttribute('data-md-scrollfix', '')
      list.querySelectorAll('li').forEach((item, itemIndex) => {
        visitListItem(item, path + '-' + itemIndex, level)
      })
    }

    visitList(root, 'nav-' + rootIndex, 1)
    var projectVersion = document.getElementById("project.version")
    if (projectVersion) {
      root.appendChild(projectVersion)
    }
    root.parentNode.style.visibility = 'visible'
  })

  document.querySelectorAll('.md-sidebar--secondary .md-nav--secondary > ul').forEach(tocRoot => {
    function visitListItem(item) {
      item.classList.add('md-nav__item')
      item.querySelectorAll(':scope> a').forEach(link => {
        link.classList.add('md-nav__link')
        link.setAttribute('data-md-state', '')
      })
      item.querySelectorAll(':scope > ul').forEach(list => {
        visitList(list)
      })
    }

    function visitList(list) {
      list.classList.add('md-nav__list')
      list.querySelectorAll(':scope > li').forEach(item => {
        visitListItem(item)
      })
    }

    var parent = tocRoot.parentNode
    parent.removeChild(tocRoot)

    tocRoot.querySelectorAll(':scope > li > ul').forEach(list => {
      parent.append(list)
      list.setAttribute('data-md-scrollfix', '')
      visitList(list)
    })

    parent.style.visibility = 'visible';
  })

  document.querySelectorAll('dl').forEach(dl => {
    const tabContents = dl.querySelectorAll(':scope > dd > pre')
    if (tabContents.length > 0) {
      dl.classList.add('mdc-tab-bar')
      var first = true
      var contentContainer = document.createElement('div')
      contentContainer.classList.add('mdc-tab-content-container')

      tabContents.forEach(pre => {
        var dd = pre.parentNode
        var dt = dd.previousSibling
        while (dt.nodeType != dt.ELEMENT_NODE) {
          dt = dt.previousSibling
        }

        var tabContent = document.createElement('div')
        tabContent.classList.add('mdc-tab-content')
        contentContainer.appendChild(tabContent)
        while (dd.childNodes.length > 0) {
          tabContent.appendChild(dd.childNodes[0]);
        }
        dl.removeChild(dd)

        dt.classList.add('mdc-tab')
        if (first) {
          dt.classList.add('mdc-tab--active')
          tabContent.classList.add('mdc-tab-content--active')
        }
        first = false
        dt.onclick = event => {
          dl.querySelectorAll(':scope .mdc-tab--active').forEach(active => {
            active.classList.remove('mdc-tab--active')
          })
          contentContainer.querySelectorAll(':scope .mdc-tab-content--active').forEach(active => {
            active.classList.remove('mdc-tab-content--active')
          })
          dt.classList.add('mdc-tab--active')
          tabContent.classList.add('mdc-tab-content--active')
        }
      })

      if (dl.nextSibling)
        dl.parentNode.insertBefore(contentContainer, dl.nextSibling)
      else
        dl.parentNode.appendChild(contentContainer)
    }
  })
}

initParadoxMaterialTheme()
