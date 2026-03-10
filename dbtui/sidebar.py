from textual.widgets import Tree


class SideBar(Tree):
    def __init__(self, project=None, include=("model", "source", "seed"), **kwargs):
        """
        SideBar consumes a DBTProject (single source-of-truth) and populates
        models/sources/seeds from its manifest JSON.

        Args:
            project: DBTProject instance (or None)
            include: iterable of resource types to show (subset of "model","source","seed")
        """
        super().__init__(**kwargs)
        self.project = project
        self.include = set(include)
        self.show_root = False
        self.guide = 2

    def on_mount(self):
        self.populate()

    def populate(self):
        # Clear any existing root children if re-populating
        try:
            # textual Tree API supports clearing the whole tree from root
            self.clear()
        except Exception:
            pass

        if not self.project:
            return

        if "model" in self.include:
            models = self.project.get_models()
            model_tree = self.root.add(
                f"Models ({len(models)})", expand=len(models) > 0
            )
            for uid, node in sorted(
                models.items(), key=lambda kv: kv[1].get("name", kv[0])
            ):
                display_name = node.get("name", uid)
                model_tree.add_leaf(display_name, data=node)

        if "source" in self.include:
            sources = self.project.get_sources()
            source_tree = self.root.add(
                f"Sources ({len(sources)})", expand=len(sources) > 0
            )
            for uid, src in sorted(
                sources.items(), key=lambda kv: kv[1].get("name", kv[0])
            ):
                display_name = src.get("name", uid)
                source_tree.add_leaf(display_name, data=src)

        if "seed" in self.include:
            seeds = self.project.get_seeds()
            seed_tree = self.root.add(f"Seeds ({len(seeds)})", expand=len(seeds) > 0)
            for uid, node in sorted(
                seeds.items(), key=lambda kv: kv[1].get("name", kv[0])
            ):
                display_name = node.get("name", uid)
                seed_tree.add_leaf(display_name, data=node)
