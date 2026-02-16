import { task } from "@basicblock/trigger-sdk/v3";
import * as mupdf from "mupdf";

export const helloWorld = task({
  id: "helloWorld",
  run: async () => {
    console.log("Hello, World!", {
      metaformat: mupdf.PDFDocument.META_FORMAT,
    });
  },
});
