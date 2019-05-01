xsltproc /usr/share/xml/docbook-xsl-1.79.1/fo/docbook.xsl book.xml > book.fo && fop -fo book.fo -pdf book.pdf
