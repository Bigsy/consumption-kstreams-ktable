# consumption-kstreams-ktable

Happy path contract+tariff+consumption -> charge

Process
* Group/Aggregate stream of tariffs to key (because variable has same key) -> ktable
* Join tariffs onto Contracts stream, reduce -> ktable
* Join stream of consumption on to Contract+tariff ktable
* Output to stream of charges

## Usage

lein test