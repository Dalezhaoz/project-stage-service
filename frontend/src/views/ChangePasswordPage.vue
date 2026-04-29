<template>
  <section class="auth-page">
    <div class="auth-card">
      <div class="auth-card-copy">
        <p class="eyebrow">Password Update</p>
        <h3>首次登录请先修改密码</h3>
        <p>当前账号已登录，但系统要求你先修改密码，修改完成后会自动进入新版控制台。</p>
      </div>

      <form class="auth-form" @submit.prevent="submit">
        <label class="field">
          <span>当前密码</span>
          <input v-model="form.currentPassword" type="password" autocomplete="current-password" placeholder="请输入当前密码" />
        </label>

        <label class="field">
          <span>新密码</span>
          <input v-model="form.newPassword" type="password" autocomplete="new-password" placeholder="请输入新密码" />
        </label>

        <label class="field">
          <span>确认新密码</span>
          <input v-model="form.confirmPassword" type="password" autocomplete="new-password" placeholder="再次输入新密码" />
        </label>

        <div v-if="errorMessage" class="auth-error">{{ errorMessage }}</div>

        <button class="button primary auth-submit" type="submit" :disabled="submitting">
          {{ submitting ? "提交中..." : "保存新密码" }}
        </button>
      </form>
    </div>
  </section>
</template>

<script setup lang="ts">
import { reactive, ref } from "vue";
import { useRouter } from "vue-router";
import { useAuthState } from "@/stores/auth";

const router = useRouter();
const auth = useAuthState();
const submitting = ref(false);
const errorMessage = ref("");

const form = reactive({
  currentPassword: "",
  newPassword: "",
  confirmPassword: ""
});

async function submit() {
  errorMessage.value = "";
  submitting.value = true;

  try {
    if (!form.currentPassword || !form.newPassword || !form.confirmPassword) {
      throw new Error("请完整填写密码信息。");
    }

    await auth.changeOwnPassword({
      currentPassword: form.currentPassword,
      newPassword: form.newPassword,
      confirmPassword: form.confirmPassword
    });

    await router.replace("/");
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : "修改密码失败";
  } finally {
    submitting.value = false;
  }
}
</script>
