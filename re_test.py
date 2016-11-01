#! /usr/bin/python
import re
tweetText= 'VERY VERY URGENT : doctor around in mudichur area help needed? , now pregnant lady in  labor !! Please share ! #ChennaiRainsHelp https://t.co/ovvaesu9vr'
pattern = 'needed '
if re.search(pattern,tweetText,re.I):
    print 'true'
else:
    print 'false'
