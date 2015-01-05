<?xml version='1.0'?> 
<xsl:stylesheet  
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
    xmlns:fo="http://www.w3.org/1999/XSL/Format"
    xmlns:fox="http://xmlgraphics.apache.org/fop/extensions"
    version="1.0"> 

<xsl:import href="/home/smarcus/writing/xxe-pro-5_5_0/addon/config/docbook/xsl/fo/docbook.xsl"/>
<!-- <xsl:import href="/usr/share/xml/docbook/stylesheet/docbook-xsl/fo/docbook.xsl"/> -->

<!-- SJM: set up email tag to generate nice-looking links -->
<xsl:param name="email.mailto.enabled" select="1" />
<xsl:param name="email.delimiters.enabled" select="0" />

<xsl:param name="admon.textlabel" select="0"></xsl:param>

<xsl:attribute-set name="admonition.properties">
 <xsl:attribute name="border">0.5pt solid black</xsl:attribute>
  <xsl:attribute name="padding">0.1in</xsl:attribute>
</xsl:attribute-set>

<!-- SJM: set link appearance to blue, underlined text -->
<xsl:attribute-set name="xref.properties">
  <xsl:attribute name="color">#000088</xsl:attribute>
  <xsl:attribute name="text-decoration">underline</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="section.title.properties">
  <xsl:attribute name="space-before.minimum">50pt</xsl:attribute>
  <xsl:attribute name="space-before.optimum">55pt</xsl:attribute>
  <xsl:attribute name="space-before.maximum">60pt</xsl:attribute>
  <xsl:attribute name="space-after.minimum">0.5em</xsl:attribute>
  <xsl:attribute name="space-after.optimum">1em</xsl:attribute>
  <xsl:attribute name="space-after.maximum">2em</xsl:attribute>
  <xsl:attribute name="keep-together.within-column">auto</xsl:attribute>
</xsl:attribute-set>

</xsl:stylesheet>  
