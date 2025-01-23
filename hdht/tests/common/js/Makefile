# path to the js submodule
HYPERDHT_DIR := hyperdht

HDHT_SOURCES = $(shell test -d $(HYPERDHT_DIR) && find $(HYPERDHT_DIR) -type f -name "*.js")

node_modules: package.json $(HYPERDHT_DIR) $(HDHT_SOURCES)
	yarn install

# pull in the submodule if it doesn't exist
$(HYPERDHT_DIR):
	git submodule update --init --recursive

