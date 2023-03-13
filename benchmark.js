/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import { fileURLToPath } from "url";

// snippet-start:[javascript.v3.s3.scenarios.multipartdownload]
import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { createWriteStream } from "fs";

const s3Client = new S3Client({ region: "us-east-1" })

export const getObjectRange = ({ bucket, key, start, end }) => {
    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: key,
      Range: `bytes=${start}-${end}`,
    });
  
    return s3Client.send(command);
  };

  
export const getRangeAndLength = (contentRange) => {
    const [range, length] = contentRange.split("/");
    const [start, end] = range.split("-");
    return {
      start: parseInt(start),
      end: parseInt(end),
      length: parseInt(length),
    };
  };
  
  export const isComplete = ({ end, length }) => end === length - 1;

  
const downloadInChunks = async ({ bucket, key, chunkSize }) => {
    const oneMB = 1024 * 1024;
    const writeStream = createWriteStream(
      fileURLToPath(new URL(`./${chunkSize}MB_${key}`, import.meta.url))
    ).on("error", (err) => console.error(err));
  
    let rangeAndLength = { start: -1, end: -1, length: -1 };
    const chunkBytes = chunkSize * oneMB;
  
    while (!isComplete(rangeAndLength)) {
      const { end } = rangeAndLength;
      const nextRange = { start: end + 1, end: Math.ceil(end + chunkBytes) };
  
      const { ContentRange, Body } = await getObjectRange({
        bucket,
        key,
        ...nextRange,
      });
  
      writeStream.write(await Body.transformToByteArray());
      rangeAndLength = getRangeAndLength(ContentRange);
    }
  };

  
export const main = async () => {
    const configs = [
      { chunkSize: 100, label: "100MB" },
      { chunkSize: 10, label: "10MB" },
      { chunkSize: 1, label: "1MB" },
      { chunkSize: 0.1, label: "0.1MB" },
    ];
  
    const bucket = "benchmark-mb";
    const key = "data.txt";
  
    for (const { chunkSize, label } of configs) {
        console.log(`Starting download of ${key} with chunk size ${label}`);
        const startTime = new Date();
        await downloadInChunks({ bucket, key, chunkSize });
        const endTime = new Date();
        console.log(`Download of ${key} with chunk size ${label} is complete. Time: ${endTime - startTime}`);
    }
  };
  
  // snippet-end:[javascript.v3.s3.scenarios.multipartdownload]
  
  // Invoke main function if this file was run directly.
  if (process.argv[1] === fileURLToPath(import.meta.url)) {
    main();
  }
  