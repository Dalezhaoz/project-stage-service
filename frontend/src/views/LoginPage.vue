<template>
  <section class="auth-page">
    <div class="auth-card">
      <div class="auth-card-copy">
        <p class="eyebrow">Authentication</p>
        <h3>{{ pageTitle }}</h3>
        <p>{{ pageDescription }}</p>
      </div>

      <form class="auth-form" @submit.prevent="submit">
        <label class="field">
          <span>用户名</span>
          <input v-model.trim="form.username" type="text" autocomplete="username" placeholder="请输入用户名" />
        </label>

        <label class="field">
          <span>密码</span>
          <input v-model="form.password" type="password" autocomplete="current-password" placeholder="请输入密码" />
        </label>

        <label v-if="mode === 'setup'" class="field">
          <span>确认密码</span>
          <input v-model="form.confirmPassword" type="password" autocomplete="new-password" placeholder="再次输入密码" />
        </label>

        <div class="auth-helper">
          <span>{{ helperText }}</span>
        </div>

        <div v-if="errorMessage" class="auth-error">{{ errorMessage }}</div>

        <button class="button primary auth-submit" type="submit" :disabled="submitting">
          {{ submitting ? "提交中..." : submitLabel }}
        </button>
      </form>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, reactive, ref } from "vue";
import { useRouter } from "vue-router";
import { useAuthState } from "@/stores/auth";

const router = useRouter();
const auth = useAuthState();
const mode = computed(() => (auth.authStatus.value?.hasAccount === false ? "setup" : "login"));
const submitting = ref(false);
const errorMessage = ref("");

const form = reactive({
  username: "",
  password: "",
  confirmPassword: ""
});

const pageTitle = computed(() => (mode.value === "setup" ? "初始化管理员账户" : "登录项目阶段控制台"));
const pageDescription = computed(() =>
  mode.value === "setup"
    ? "系统还没有管理员账号，请先完成首次初始化。初始化成功后会自动登录。"
    : "使用现有账号登录新版控制台。登录成功后会直接进入新版首页。"
);
const helperText = computed(() =>
  mode.value === "setup"
    ? "首次初始化会创建管理员账号。"
    : "如果提示首次登录需要修改密码，登录后会自动跳转到修改密码页。"
);
const submitLabel = computed(() => (mode.value === "setup" ? "初始化并登录" : "登录"));

async function submit() {
  errorMessage.value = "";
  submitting.value = true;

  try {
    if (!form.username || !form.password) {
      throw new Error("请输入用户名和密码。");
    }

    if (mode.value === "setup") {
      if (!form.confirmPassword) {
        throw new Error("请再次输入确认密码。");
      }

      await auth.initializeAccount({
        username: form.username,
        password: form.password,
        confirmPassword: form.confirmPassword
      });
    } else {
      await auth.loginWithPassword({
        username: form.username,
        password: form.password
      });
    }

    if (auth.authStatus.value?.forcePasswordChange) {
      await router.replace("/change-password");
      return;
    }

    await router.replace("/");
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : "登录失败";
  } finally {
    submitting.value = false;
  }
}
</script>
