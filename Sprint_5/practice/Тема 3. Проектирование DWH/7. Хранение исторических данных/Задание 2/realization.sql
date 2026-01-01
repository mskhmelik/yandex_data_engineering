ALTER TABLE de.public.sales
DROP CONSTRAINT sales_products_product_id_fk;

ALTER TABLE de.public.products
DROP CONSTRAINT products_pk;

ALTER TABLE de.public.products
ADD COLUMN id serial;

ALTER TABLE de.public.products
ADD CONSTRAINT products_pk PRIMARY KEY (id);

ALTER TABLE de.public.products
ADD COLUMN valid_from timestamptz NOT NULL;

ALTER TABLE de.public.products
ADD COLUMN valid_to timestamptz NOT NULL;

ALTER TABLE de.public.sales
ADD CONSTRAINT sales_products_id_fk
FOREIGN KEY (product_id)
REFERENCES de.public.products (id);
