CREATE DATABASE /*!32312 IF NOT EXISTS*/ `examples` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE `examples`;

DROP TABLE IF EXISTS `sales`;

CREATE TABLE `sales` (
    id INT PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL,
    descripcion VARCHAR(255) NOT NULL,
    direccion VARCHAR(255) NOT NULL
);

INSERT INTO sales (id, nombre, descripcion, direccion) VALUES
(1, 'Telefono movil', 'iPhone X en perfectas condiciones', 'Calle Principal, 123'),
(2, 'Camara digital', 'Nikon D850 con lente 50mm', 'Avenida Central, 456'),
(3, 'Libro de informatica', 'Recetas saludables para el dia a dia', 'Calle Secundaria, 789'),
(4, 'Bicicleta', 'Marca: Trek, Talla: M', 'Calle Principal, 456'),
(5, 'Juego de mesa', 'Monopoly edicion especial', 'Avenida Principal, 789');