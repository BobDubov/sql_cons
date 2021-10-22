import xml.etree.cElementTree as ET

tree = ET.ElementTree(file='CONS#1_000073.XML')
# print(tree.getroot())
root = tree.getroot()
# print("tag=%s, attrib=%s" % (root.tag, root.attrib))
ib = root.find('ib')
for i in ib.findall("*"):
    print(i.tag, i.attrib['nDistr'], i.attrib['nCat'] ,i.find("updates/u1").attrib["date"])

