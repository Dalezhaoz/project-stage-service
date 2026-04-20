import { createRouter, createWebHistory } from "vue-router";
import OverviewPage from "./views/OverviewPage.vue";
import PlaygroundPage from "./views/PlaygroundPage.vue";

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: "/",
      component: OverviewPage
    },
    {
      path: "/playground",
      component: PlaygroundPage
    }
  ]
});

export default router;
