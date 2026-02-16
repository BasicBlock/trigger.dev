import { task } from "@basicblock/trigger-sdk/v3";
import { renderExampleEmail } from "@repo/email";

export const reactEmail = task({
  id: "react-email",
  run: async () => {
    return await renderExampleEmail();
  },
});
