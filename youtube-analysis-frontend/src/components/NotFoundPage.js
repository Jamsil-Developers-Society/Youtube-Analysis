// install (please try to align the version of installed @nivo packages)
// yarn add @nivo/pie
// import { ResponsivePie } from "@nivo/pie";

import { Box, Typography } from "@mui/material";

function NotFoundPage() {
  return (
    <>
      <Box>
        <Typography
          variant="body2"
          sx={{ color: "blue", p: 2, backgroundColor: "black" }}
        >
          404 Not Found
        </Typography>
      </Box>
    </>
  );
}

export default NotFoundPage;
