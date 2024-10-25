import { Box, Grid2, Paper, Typography } from "@mui/material";
import Progress from "./Progress";
import Bumpchart from "./Bumpchart";
import { useState } from "react";
import Piechart from "./Piechart";
import Linechart from "./Linechart";

function EmotionAnalysisPage() {
  const [arrEmotionAnalysisData1, setEmotionAnalysisData1] = useState([
    { id: "cola", value: 324 },
    { id: "cidar", value: 88 },
    { id: "fanta", value: 221 },
  ]);
  const [arrEmotionAnalysisData2, setEmotionAnalysisData2] = useState([
    {
      id: "japan",
      color: "hsl(122, 70%, 50%)",
      data: [
        {
          x: "plane",
          y: 115,
        },
        {
          x: "helicopter",
          y: 254,
        },
        {
          x: "boat",
          y: 209,
        },
        {
          x: "train",
          y: 52,
        },
        {
          x: "subway",
          y: 189,
        },
        {
          x: "bus",
          y: 197,
        },
        {
          x: "car",
          y: 79,
        },
        {
          x: "moto",
          y: 252,
        },
        {
          x: "bicycle",
          y: 198,
        },
        {
          x: "horse",
          y: 177,
        },
        {
          x: "skateboard",
          y: 270,
        },
        {
          x: "others",
          y: 175,
        },
      ],
    },
    {
      id: "france",
      color: "hsl(303, 70%, 50%)",
      data: [
        {
          x: "plane",
          y: 210,
        },
        {
          x: "helicopter",
          y: 202,
        },
        {
          x: "boat",
          y: 61,
        },
        {
          x: "train",
          y: 162,
        },
        {
          x: "subway",
          y: 27,
        },
        {
          x: "bus",
          y: 37,
        },
        {
          x: "car",
          y: 107,
        },
        {
          x: "moto",
          y: 297,
        },
        {
          x: "bicycle",
          y: 196,
        },
        {
          x: "horse",
          y: 13,
        },
        {
          x: "skateboard",
          y: 232,
        },
        {
          x: "others",
          y: 82,
        },
      ],
    },
    {
      id: "us",
      color: "hsl(48, 70%, 50%)",
      data: [
        {
          x: "plane",
          y: 248,
        },
        {
          x: "helicopter",
          y: 226,
        },
        {
          x: "boat",
          y: 214,
        },
        {
          x: "train",
          y: 276,
        },
        {
          x: "subway",
          y: 295,
        },
        {
          x: "bus",
          y: 51,
        },
        {
          x: "car",
          y: 102,
        },
        {
          x: "moto",
          y: 244,
        },
        {
          x: "bicycle",
          y: 267,
        },
        {
          x: "horse",
          y: 110,
        },
        {
          x: "skateboard",
          y: 192,
        },
        {
          x: "others",
          y: 148,
        },
      ],
    },
    {
      id: "germany",
      color: "hsl(290, 70%, 50%)",
      data: [
        {
          x: "plane",
          y: 63,
        },
        {
          x: "helicopter",
          y: 127,
        },
        {
          x: "boat",
          y: 106,
        },
        {
          x: "train",
          y: 110,
        },
        {
          x: "subway",
          y: 201,
        },
        {
          x: "bus",
          y: 27,
        },
        {
          x: "car",
          y: 197,
        },
        {
          x: "moto",
          y: 173,
        },
        {
          x: "bicycle",
          y: 111,
        },
        {
          x: "horse",
          y: 123,
        },
        {
          x: "skateboard",
          y: 3,
        },
        {
          x: "others",
          y: 73,
        },
      ],
    },
    {
      id: "norway",
      color: "hsl(13, 70%, 50%)",
      data: [
        {
          x: "plane",
          y: 61,
        },
        {
          x: "helicopter",
          y: 200,
        },
        {
          x: "boat",
          y: 261,
        },
        {
          x: "train",
          y: 237,
        },
        {
          x: "subway",
          y: 187,
        },
        {
          x: "bus",
          y: 104,
        },
        {
          x: "car",
          y: 197,
        },
        {
          x: "moto",
          y: 52,
        },
        {
          x: "bicycle",
          y: 266,
        },
        {
          x: "horse",
          y: 55,
        },
        {
          x: "skateboard",
          y: 68,
        },
        {
          x: "others",
          y: 238,
        },
      ],
    },
  ]);
  const [progress, setProgress] = useState(false);

  return (
    <>
      <Progress progress={progress} />
      <Box xs={{ p: 2 }}>
        <Grid2 container spacing={2} sx={{ p: 2 }}>
          <Grid2 size={{ xs: 6, md: 6 }}>
            <Paper>
              <Piechart data={arrEmotionAnalysisData1} height={"500px"} />
            </Paper>
          </Grid2>
          <Grid2 size={{ xs: 6, md: 6 }}>
            <Paper>
              <Linechart data={arrEmotionAnalysisData2} height={"500px"} />
            </Paper>
          </Grid2>
        </Grid2>
        {/* container end */}
      </Box>
    </>
  );
}

export default EmotionAnalysisPage;
