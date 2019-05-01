xsltproc /usr/share/xml/docbook-xsl-1.79.1/fo/docbook.xsl manual.xml > manual.fo && fop -fo manual.fo -pdf manual.pdf
