const fs = require('fs');
const path = require('path');

const src = path.resolve(__dirname, '..', 'node_modules', '@uswds', 'uswds', 'dist');
const dest = path.resolve(__dirname, '..', 'static', 'vendor', 'uswds');

async function copyDirectory(source, destination) {
  await fs.promises.mkdir(destination, { recursive: true });
  const entries = await fs.promises.readdir(source, { withFileTypes: true });

  await Promise.all(
    entries.map(async (entry) => {
      const sourcePath = path.join(source, entry.name);
      const destinationPath = path.join(destination, entry.name);

      if (entry.isDirectory()) {
        await copyDirectory(sourcePath, destinationPath);
      } else if (entry.isSymbolicLink()) {
        const link = await fs.promises.readlink(sourcePath);
        await fs.promises.symlink(link, destinationPath);
      } else {
        await fs.promises.copyFile(sourcePath, destinationPath);
      }
    }),
  );
}

async function removeDirectory(target) {
  await fs.promises.rm(target, { recursive: true, force: true });
}

async function run() {
  try {
    await fs.promises.access(src, fs.constants.R_OK);
  } catch (error) {
    console.error('USWDS source not found. Did you run `npm install`?');
    process.exitCode = 1;
    return;
  }

  await removeDirectory(dest);
  await copyDirectory(src, dest);
  console.log(`USWDS assets copied to ${dest}`);
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
