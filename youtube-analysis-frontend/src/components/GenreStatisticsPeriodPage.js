import { Box, Grid2, Paper, Typography } from "@mui/material";
import Progress from "./Progress";
import Piechart from "./Piechart";
import { useEffect, useState } from "react";
import { selectGenrePeriodData } from "../services/videoAPI";
import Bumpchart from "./Bumpchart";

function GenreStatisticsPeriodPage() {
  const [arrPeriodData, setArrPeriodData] = useState([]);
  const [progress, setProgress] = useState(true);

  useEffect(() => {
    console.log("데이터 로딩 중..");
    fn();
    setProgress(false);
  }, []);

  const fn = () => {
    selectGenrePeriodData().then((res) => {
      // console.log(res?.data);
      const data = JSON.parse(res.data).map((item) => ({
        id: item.category_title,
        data: [],
      }));
      setArrPeriodData(data);
      setProgress(false);
    });
  };

  return (
    <>
      <Progress progress={progress} />
      <Box xs={{ p: 2 }}>
        <Grid2 container spacing={2} sx={{ p: 2 }}>
          <Grid2 size={{ xs: 6, md: 12 }}>
            <Paper>
              <Bumpchart data={arrPeriodData} height={"500px"} />
            </Paper>
          </Grid2>
        </Grid2>
        {/* container end */}
      </Box>
    </>
  );
}

export default GenreStatisticsPeriodPage;
