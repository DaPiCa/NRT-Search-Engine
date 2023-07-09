CREATE DATABASE /*!32312 IF NOT EXISTS*/ `examples` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci */;

USE `examples`;

DROP TABLE IF EXISTS `sales`;

CREATE TABLE `sales` (
    id INT PRIMARY KEY,
    nombre VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    descripcion VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    direccion VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL
);

INSERT INTO sales (id, nombre, descripcion, direccion) VALUES
(1, 'Teléfono móvil', 'iPhone X en perfectas condiciones', 'Calle Principal, 123'),
(2, 'Cámara digital', 'Nikon D850 con lente 50mm', 'Avenida Central, 456'),
(3, 'Libro de cocina', 'Recetas saludables para el día a día', 'Calle Secundaria, 789'),
(4, 'Bicicleta de montaña', 'Marca: Trek, Talla: M', 'Calle Principal, 456'),
(5, 'Juego de mesa', 'Monopoly edición especial', 'Avenida Principal, 789');
