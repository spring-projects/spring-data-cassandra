create table if not exists book (isbn text, title text, author text, pages int, PRIMARY KEY (isbn));
create table if not exists book_alt (isbn text, title text, author text, pages int, PRIMARY KEY (isbn));
truncate table book;
truncate table book_alt;
insert into book (isbn, title, author, pages) values ('999999999', 'Book of Nines', 'Nine Nine', 999);
