PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;

DROP TABLE IF EXISTS OrderDetails;
DROP TABLE IF EXISTS Orders;
DROP TABLE IF EXISTS Products;
DROP TABLE IF EXISTS Customers;
DROP TABLE IF EXISTS Employees;
DROP TABLE IF EXISTS Shippers;
DROP TABLE IF EXISTS Suppliers;
DROP TABLE IF EXISTS Categories;

CREATE TABLE Categories (
  CategoryID INTEGER NOT NULL PRIMARY KEY,
  CategoryName TEXT NOT NULL,
  Description TEXT
);

CREATE TABLE Customers (
  CustomerID INTEGER NOT NULL PRIMARY KEY,
  CustomerName TEXT NOT NULL,
  ContactName TEXT,
  Address TEXT,
  City TEXT,
  PostalCode TEXT,
  Country TEXT
);

CREATE TABLE Employees (
  EmployeeID INTEGER NOT NULL PRIMARY KEY,
  LastName TEXT NOT NULL,
  FirstName TEXT NOT NULL,
  BirthDate DATE,
  Photo TEXT,
  Notes TEXT
);

CREATE TABLE Shippers (
  ShipperID INTEGER NOT NULL PRIMARY KEY,
  ShipperName TEXT NOT NULL,
  Phone TEXT
);

CREATE TABLE Suppliers (
  SupplierID INTEGER NOT NULL PRIMARY KEY,
  SupplierName TEXT NOT NULL,
  ContactName TEXT,
  Address TEXT,
  City TEXT,
  PostalCode TEXT,
  Country TEXT,
  Phone TEXT
);

CREATE TABLE Products (
  ProductID INTEGER NOT NULL PRIMARY KEY,
  ProductName TEXT NOT NULL,
  SupplierID INTEGER NOT NULL,
  CategoryID INTEGER NOT NULL,
  Unit TEXT,
  Price DECIMAL(10,2),
  FOREIGN KEY (SupplierID) REFERENCES Suppliers(SupplierID),
  FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID)
);

CREATE TABLE Orders (
  OrderID INTEGER NOT NULL PRIMARY KEY,
  CustomerID INTEGER NOT NULL,
  EmployeeID INTEGER NOT NULL,
  OrderDate DATE,
  ShipperID INTEGER NOT NULL,
  FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
  FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID),
  FOREIGN KEY (ShipperID) REFERENCES Shippers(ShipperID)
);

CREATE TABLE OrderDetails (
  OrderDetailID INTEGER NOT NULL PRIMARY KEY,
  OrderID INTEGER NOT NULL,
  ProductID INTEGER NOT NULL,
  Quantity INTEGER NOT NULL,
  FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
  FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

INSERT INTO Categories (CategoryID, CategoryName, Description) VALUES (1, 'Bebidas', 'Refrigerantes, sucos, chás, cervejas e cachaças');
INSERT INTO Categories (CategoryID, CategoryName, Description) VALUES (2, 'Condimentos', 'Molhos, temperos, conservas e especiarias');
INSERT INTO Categories (CategoryID, CategoryName, Description) VALUES (3, 'Confeitaria', 'Doces, balas, chocolates e pães doces');
INSERT INTO Categories (CategoryID, CategoryName, Description) VALUES (4, 'Laticínios', 'Queijos e derivados do leite');
INSERT INTO Categories (CategoryID, CategoryName, Description) VALUES (5, 'Grãos e Cereais', 'Pães, biscoitos, massas e cereais');
INSERT INTO Categories (CategoryID, CategoryName, Description) VALUES (6, 'Carnes e Aves', 'Carnes e aves preparadas');
INSERT INTO Categories (CategoryID, CategoryName, Description) VALUES (7, 'Hortifruti', 'Frutas, verduras e legumes frescos');
INSERT INTO Categories (CategoryID, CategoryName, Description) VALUES (8, 'Frutos do Mar', 'Peixes e frutos do mar');

INSERT INTO Customers (CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country) VALUES (1, 'Pão de Açúcar Comércio', 'Joana Silva', 'Av. Paulista, 1578', 'São Paulo', '01310-100', 'Brasil');
INSERT INTO Customers (CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country) VALUES (2, 'empório santa luzia', 'Ana Lima', 'Rua Haddock Lobo, 1626', 'são paulo', '01414-002', 'Brasil');
INSERT INTO Customers (CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country) VALUES (3, 'CHURRASCARIA DO GAÚCHO', 'Marcos Alves', 'Av. Borges de Medeiros, 701', 'Porto Alegre', '90020-021', 'Brasil');
INSERT INTO Customers (CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country) VALUES (4, 'Casa de Carnes Mineira', 'Fernanda Rocha', 'Av. Afonso Pena, 3000', 'Belo Horizonte', '30130-009', 'Brasil');
INSERT INTO Customers (CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country) VALUES (5, 'mercearia carioca', 'Pedro Costa', 'Rua Visconde de Pirajá, 414', 'rio de janeiro', '22410-002', 'Brasil');
INSERT INTO Customers (CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country) VALUES (6, 'PADARIA NORDESTINA', 'Carla Souza', 'Av. Dantas Barreto, 500', 'Recife', '50010-050', 'Brasil');
INSERT INTO Customers (CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country) VALUES (7, 'Empório Paraense', 'Luís Torres', 'Av. Nazaré, 100', 'belém', '66035-170', 'Brasil');
INSERT INTO Customers (CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country) VALUES (8, 'Supermercado Bahia', 'Renata Batista', 'Av. Tancredo Neves, 1000', 'Salvador', '41820-021', 'Brasil');

INSERT INTO Employees (EmployeeID, LastName, FirstName, BirthDate, Photo, Notes) VALUES (1, 'Silva', 'Carlos', '1975-03-15', 'FuncID1.pic', 'Carlos é bacharel em Administração pela USP. Concluiu o curso de Gestão de Vendas. É membro da Associação Brasileira de Marketing.');
INSERT INTO Employees (EmployeeID, LastName, FirstName, BirthDate, Photo, Notes) VALUES (2, 'Santos', 'Ana Paula', '1980-06-22', 'FuncID2.pic', 'Ana Paula possui MBA em Marketing pela FGV e graduação pela UNICAMP. É fluente em inglês e espanhol. Ingressou como representante de vendas e foi promovida a gerente regional. É membro da Associação Brasileira de Marketing e do Instituto Brasileiro de Executivos de Vendas.');
INSERT INTO Employees (EmployeeID, LastName, FirstName, BirthDate, Photo, Notes) VALUES (3, 'Oliveira', 'Fernanda', '1990-11-08', 'FuncID3.pic', 'Fernanda possui graduação em Nutrição pela UFRJ. Concluiu certificação em gestão de alimentos. Iniciou como assistente comercial e foi promovida a representante de vendas.');
INSERT INTO Employees (EmployeeID, LastName, FirstName, BirthDate, Photo, Notes) VALUES (4, 'Souza', 'Ricardo', '1968-09-12', 'FuncID4.pic', 'Ricardo possui graduação em Administração pela PUC-SP e pós-graduação pelo INSPER. Foi alocado temporariamente na filial de Manaus antes de retornar à sede em São Paulo.');
INSERT INTO Employees (EmployeeID, LastName, FirstName, BirthDate, Photo, Notes) VALUES (5, 'Ferreira', 'Marcos', '1972-07-30', 'FuncID5.pic', 'Marcos se formou na ESPM com graduação em Publicidade. Ao ingressar como representante, passou por treinamento em São Paulo e retornou ao Rio de Janeiro, onde foi promovido a gerente de vendas. Concluiu os cursos de Televendas e Gestão Comercial Internacional. É fluente em inglês.');
INSERT INTO Employees (EmployeeID, LastName, FirstName, BirthDate, Photo, Notes) VALUES (6, 'Pereira', 'Juliana', '1985-04-18', 'FuncID6.pic', 'Juliana é graduada em Economia pela UFMG e tem MBA pela FIA. Realizou os cursos de Negociação Avançada e Gestão Comercial. Fala inglês e espanhol fluentemente.');
INSERT INTO Employees (EmployeeID, LastName, FirstName, BirthDate, Photo, Notes) VALUES (7, 'Costa', 'Roberto', '1978-12-03', 'FuncID7.pic', 'Roberto atuou no serviço público antes de ingressar na empresa. Concluiu especialização em Gestão de Negócios pela FGV e foi transferido para a filial de Curitiba.');
INSERT INTO Employees (EmployeeID, LastName, FirstName, BirthDate, Photo, Notes) VALUES (8, 'Almeida', 'Larissa', '1982-02-27', 'FuncID8.pic', 'Larissa possui graduação em Psicologia pela UFSC. Realizou curso de negociação comercial. Lê e escreve em inglês e espanhol.');
INSERT INTO Employees (EmployeeID, LastName, FirstName, BirthDate, Photo, Notes) VALUES (9, 'Rodrigues', 'Patricia', '1992-08-14', 'FuncID9.pic', 'Patricia possui graduação em Letras pela UNESP. É fluente em inglês e francês.');
INSERT INTO Employees (EmployeeID, LastName, FirstName, BirthDate, Photo, Notes) VALUES (10, 'Lima', 'Bruno', '1970-05-09', 'FuncID10.pic', 'Um velho amigo.');

INSERT INTO Shippers (ShipperID, ShipperName, Phone) VALUES (1, 'Correios Express', '(11) 4003-0100');
INSERT INTO Shippers (ShipperID, ShipperName, Phone) VALUES (2, 'Jadlog Transportes', '(11) 3003-0800');
INSERT INTO Shippers (ShipperID, ShipperName, Phone) VALUES (3, 'Total Express', '(11) 4003-4567');

INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (1, 'Ambev Distribuidora', 'Rodrigo Melo', 'Rua da Várzea, 300', 'São Paulo', '01140-080', 'Brasil', '(11) 3322-1100');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (2, 'Coco Bom Nordeste', 'Sônia Barros', 'Av. Beira Mar, 1200', 'Fortaleza', '60165-092', 'Brasil', '(85) 3255-7700');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (3, 'Fazenda Bela Vista', 'Joaquim Neto', 'Estrada Rural, km 12', 'Ribeirão Preto', '14040-900', 'Brasil', '(16) 3911-5522');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (4, 'Indústria Alimentos Mineiros', 'Cláudia Torres', 'Av. dos Andradas, 500', 'Belo Horizonte', '30120-010', 'Brasil', '(31) 3224-8800');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (5, 'Laticínios Serra Gaúcha', 'Geraldo Passos', 'Rua das Hortênsias, 80', 'Caxias do Sul', '95032-000', 'Brasil', '(54) 3225-1230');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (6, 'Maré Alta Pescados', 'Simone Leal', 'Av. Oceânica, 600', 'Salvador', '40170-020', 'Brasil', '(71) 3338-9900');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (7, 'Panambi Grãos', 'Edson Cunha', 'Av. Brasil, 1800', 'Londrina', '86020-000', 'Brasil', '(43) 3322-4411');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (8, 'Cerealista Pantanal', 'Vera Mendes', 'Rua Cuiabá, 250', 'Campo Grande', '79004-050', 'Brasil', '(67) 3317-7766');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (9, 'Doces da Vovó', 'Helena Ramos', 'Rua XV de Novembro, 44', 'Curitiba', '80020-310', 'Brasil', '(41) 3223-9988');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (10, 'Tropical Bebidas', 'Marcelo Dias', 'Av. Eduardo Ribeiro, 200', 'Manaus', '69010-001', 'Brasil', '(92) 3302-5544');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (11, 'Sul Carnes Ltda', 'Patrícia Silvano', 'Rua Uruguai, 1500', 'Chapecó', '89801-050', 'Brasil', '(49) 3322-8877');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (12, 'Arrozeira Gaúcha', 'Fernando Assis', 'Av. Getúlio Vargas, 900', 'Pelotas', '96020-000', 'Brasil', '(53) 3277-4433');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (13, 'Pescados do Pará', 'Antônio Borges', 'Av. Castilhos França, 300', 'Belém', '66010-020', 'Brasil', '(91) 3212-6655');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (14, 'Queijaria Boa Vista', 'Rosana Teixeira', 'Rua da Paz, 78', 'Uberaba', '38010-050', 'Brasil', '(34) 3312-2200');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (15, 'Frios e Frescor', 'Maurício Braga', 'Av. Industrial, 1100', 'Campinas', '13035-000', 'Brasil', '(19) 3241-7788');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (16, 'Cachaçaria do Vale', 'Denise Xavier', 'Estrada do Vale, km 5', 'Salinas', '39560-000', 'Brasil', '(38) 3841-3300');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (17, 'Floresta Amazônica Alimentos', 'Pedro Guimarães', 'Av. Tocantins, 400', 'Porto Velho', '76801-000', 'Brasil', '(69) 3216-5544');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (18, 'Empório Sul Sabores', 'Cristina Lemos', 'Rua dos Imigrantes, 120', 'Blumenau', '89010-300', 'Brasil', '(47) 3321-9900');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (19, 'Indústria Pesqueira Nordeste', 'Renato Vasconcelos', 'Av. Sete de Setembro, 800', 'Natal', '59020-000', 'Brasil', '(84) 3211-7788');
INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (20, 'Delta Comercial', 'Vitória Campos', 'Av. das Américas, 5000', 'Rio de Janeiro', '22640-102', 'Brasil', '(21) 3322-1155');

INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (1, 'Cachaça Artesanal', 1, 1, 'Caixa com 12 garrafas 700ml', 85.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (2, 'Guaraná Antártica', 1, 1, '24 latas 350ml', 42.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (3, 'Molho de Pimenta', 2, 2, '12 frascos 150ml', 28.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (4, 'Tempero Baiano', 2, 2, '48 sachês 50g', 18.50);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (5, 'Farofa Pronta', 3, 2, '36 pacotes 500g', 22.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (6, 'Doce de Leite Mineiro', 3, 3, '12 potes 400g', 35.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (7, 'Mix de Castanhas', 3, 7, '12 pacotes 200g', 48.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (8, 'Brigadeiro Artesanal', 3, 3, 'Caixa 30 unidades', 55.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (9, 'Picanha Premium', 4, 6, 'Peça aprox. 1,5kg', 120.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (10, 'Camarão GG congelado', 6, 8, 'Pacote 1kg', 75.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (11, 'Queijo Minas Frescal', 5, 4, 'Peça 500g', 22.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (12, 'Queijo Coalho', 5, 4, '10 unidades 100g', 45.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (13, 'Alga Nori', 6, 8, 'Caixa 50 folhas', 12.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (14, 'Tofu Orgânico', 6, 7, '40 pacotes 300g', 28.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (15, 'Shoyu Tradicional', 6, 2, '24 frascos 150ml', 32.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (16, 'Pão de Queijo Mineiro', 7, 3, 'Caixa 500g 20 unidades', 18.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (17, 'Carne Seca Premium', 7, 6, 'Pacote 500g', 55.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (18, 'Tilápia do Pantanal', 6, 8, 'Pacote 1kg congelado', 38.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (19, 'Biscoito de Polvilho', 8, 3, 'Caixa 10 pacotes 100g', 15.00);
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price) VALUES (20, 'Goiabada Cascão', 8, 3, 'Caixa 30 embalagens 300g', 42.00);

INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipperID) VALUES (10265, 7, 2, '1996-07-25', 1);
INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipperID) VALUES (10266, 1, 4, '1996-07-26', 2);
INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipperID) VALUES (10267, 2, 1, '1996-07-27', 3);
INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipperID) VALUES (10268, 3, 5, '1996-07-28', 1);
INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipperID) VALUES (10269, 4, 3, '1996-07-29', 2);
INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipperID) VALUES (10270, 5, 6, '1996-07-30', 1);
INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipperID) VALUES (10271, 6, 7, '1996-07-31', 3);
INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipperID) VALUES (10272, 8, 8, '1996-08-01', 2);
INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipperID) VALUES (10273, 1, 9, '1996-08-02', 1);
INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipperID) VALUES (10274, 3, 10, '1996-08-03', 3);

INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (1, 10265, 11, 12);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (2, 10265, 3, 8);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (3, 10266, 5, 20);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (4, 10266, 14, 15);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (5, 10267, 1, 25);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (6, 10267, 9, 6);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (7, 10268, 7, 18);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (8, 10268, 12, 10);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (9, 10269, 2, 30);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (10, 10269, 19, 22);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (11, 10270, 4, 16);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (12, 10270, 16, 14);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (13, 10271, 6, 11);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (14, 10271, 18, 9);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (15, 10272, 8, 24);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (16, 10272, 13, 13);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (17, 10273, 10, 17);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (18, 10273, 20, 5);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (19, 10274, 15, 19);
INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity) VALUES (20, 10274, 17, 7);

COMMIT;
