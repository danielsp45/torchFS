#let show-authors(authors) = {
  set align(center)
  for author in authors {
    box(
      inset: 1em,
      [
        #author.name \
        #author.institution \
        #author.email
      ]
    )
  }
}

#let show-abstract(content) = {
  heading(level: 1, "Abstract")

  content
}

#let show-bibliography(bibfile) = {
  bibliography(bibfile)
}

#let header(conference, title) = {
  [#conference.name #conference.location #conference.date #h(1fr) #title]
}

#let show-keywords(keywords) = {
  [*Keywords: #keywords.join(", ")*]
}

#let sigplan(
  title: none,
  authors: (),
  conference: (name: "Conference", location: "New York", date: "2023"),
  abstract: none,
  keywords: (),
  copyright: "No copyright",
  isbn: "XXX-X-XXXX-XXXX-X/XX/XX",
  doi: "XX.XXXX/XXXXXXX.XXXXXXX",
  bibliography-file: none,
  content,
) = {
  set page(numbering: "1", header: header(conference, title))

  align(center, text(size: 24pt, strong(title)))

  show-authors(authors)

  columns(2,
    [
      #show-abstract(abstract)

      #show-keywords(keywords)

      #set heading(numbering: "1.")

      #content

      #if bibliography-file != none {
        show-bibliography(bibliography-file)
      }
    ]
  )
}
