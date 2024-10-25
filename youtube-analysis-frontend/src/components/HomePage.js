// install (please try to align the version of installed @nivo packages)
// yarn add @nivo/pie
// import { ResponsivePie } from "@nivo/pie";

import { Box, Grid2, Paper } from "@mui/material";
import Piechart from "./Piechart";
import Progress from "./Progress";

function HomePage() {
  return (
    <>
      <Box xs={{ p: 2 }}>
        <Grid2 container spacing={2} sx={{ p: 2 }}>
          <Grid2 size={{ xs: 6, md: 8 }}>
            <Paper style={{ height: "500px" }}>asdfg</Paper>
          </Grid2>
          <Grid2 size={{ xs: 6, md: 4 }}>
            <Paper>
              <Piechart
                data={[
                  { id: "cola", value: 324 },
                  { id: "cidar", value: 88 },
                  { id: "fanta", value: 221 },
                ]}
                // data={[
                //   { id: "Comedy", value: 2 },
                //   { id: "Gaming", value: 1 },
                //   { id: "News & Politics", value: 1 },
                //   { id: "People & Blogs", value: 1 },
                // ]}
                height={"500px"}
              />
            </Paper>
          </Grid2>
        </Grid2>
        {/* container end */}
      </Box>
    </>
  );
}

export default HomePage;
