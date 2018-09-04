import re

p = re.compile('[a-z]+')
result = p.findall("life is too short too long short")
print(result)
