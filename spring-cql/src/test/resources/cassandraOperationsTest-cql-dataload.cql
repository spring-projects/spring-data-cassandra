create table book (isbn text, title text, author text, pages int, PRIMARY KEY (isbn));
create table book_alt (isbn text, title text, author text, pages int, PRIMARY KEY (isbn));
insert into book (isbn, title, author, pages) values ('999999999', 'Book of Nines', 'Nine Nine', 999);