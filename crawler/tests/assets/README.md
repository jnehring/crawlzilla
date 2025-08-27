We created books.toscrape.com with this command

wget --recursive --no-clobber --html-extension \
     --convert-links --restrict-file-names=windows \
     --domains books.toscrape.com --no-parent \
     --accept html,htm \
     https://books.toscrape.com/

I did not download all pages