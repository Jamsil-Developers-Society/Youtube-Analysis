import * as React from "react";
import PropTypes from "prop-types";
// import Box from "@mui/material/Box";
// import Typography from "@mui/material/Typography";
import { createTheme } from "@mui/material/styles";
import SvgIcon from "@mui/material/SvgIcon";
// import DashboardIcon from "@mui/icons-material/Dashboard";
// import ShoppingCartIcon from "@mui/icons-material/ShoppingCart";
import BarChartIcon from "@mui/icons-material/BarChart";
import DescriptionIcon from "@mui/icons-material/Description";
import LayersIcon from "@mui/icons-material/Layers";
import { AppProvider } from "@toolpad/core/AppProvider";
import { DashboardLayout } from "@toolpad/core/DashboardLayout";
import HomePage from "./components/HomePage";
import NotFoundPage from "./components/NotFoundPage";
import PopularPage from "./components/PopularPage";
import StatisticsPage from "./components/GenreStatisticsRatePage";
import TotalPage from "./components/TotalPage";
import GenreStatisticsPeriodPage from "./components/GenreStatisticsPeriodPage";
import EmotionAnalysisPage from "./components/EmotionAnalysisPage";
import GenreStatisticsRatePage from "./components/GenreStatisticsRatePage";

function HomeIcon(props) {
  return (
    <SvgIcon {...props}>
      <path d="M10 20v-6h4v6h5v-8h3L12 3 2 12h3v8z" />
    </SvgIcon>
  );
}

const NAVIGATION = [
  {
    segment: "Home",
    title: "홈",
    icon: <HomeIcon />,
  },
  {
    segment: "popular",
    title: "실시간 인기 영상",
    icon: <LayersIcon />,
  },
  {
    kind: "divider",
  },
  {
    kind: "header",
    title: "Analytics",
  },
  {
    segment: "statistics",
    title: "통계",
    icon: <BarChartIcon />,
    children: [
      {
        segment: "total",
        title: "전체 비율 그래프",
        icon: <DescriptionIcon />,
      },
      {
        segment: "genre_period",
        title: "장르별 조회수 유지 기간 그래프",
        icon: <DescriptionIcon />,
      },
      {
        segment: "genre_rate",
        title: "장르별 조회수 비율 그래프",
        icon: <DescriptionIcon />,
      },
      {
        segment: "emotion",
        title: "감성 분석 그래프",
        icon: <DescriptionIcon />,
      },
    ],
  },
];

const demoTheme = createTheme({
  cssVariables: {
    colorSchemeSelector: "data-toolpad-color-scheme",
  },
  colorSchemes: { light: true, dark: true },
  breakpoints: {
    values: {
      xs: 0,
      sm: 600,
      md: 600,
      lg: 1200,
      xl: 1536,
    },
  },
});

function DemoPageContent({ pathname }) {
  // return (
  //   <Box
  //     sx={{
  //       py: 4,
  //       display: "flex",
  //       flexDirection: "column",
  //       alignItems: "center",
  //       textAlign: "center",
  //     }}
  //   >
  //     <Typography>Dashboard content for {pathname}</Typography>
  //   </Box>
  // );
  switch (pathname) {
    case "/Home":
      return <HomePage />;
    case "/dashboard":
      return <HomePage />;
    case "/popular":
      return <PopularPage />;
    case "/statistics":
      return <StatisticsPage />;
    case "/statistics/total":
      return <TotalPage />;
    case "/statistics/genre_period":
      return <GenreStatisticsPeriodPage />;
    case "/statistics/genre_rate":
      return <GenreStatisticsRatePage />;
    case "/statistics/emotion":
      return <EmotionAnalysisPage />;
    default:
      return <NotFoundPage />;
  }
}

DemoPageContent.propTypes = {
  pathname: PropTypes.string.isRequired,
};

function DashboardLayoutBasic(props) {
  const { window } = props;

  const [pathname, setPathname] = React.useState("/dashboard");

  const router = React.useMemo(() => {
    return {
      pathname,
      searchParams: new URLSearchParams(),
      navigate: (path) => setPathname(String(path)),
    };
  }, [pathname]);

  // Remove this const when copying and pasting into your project.
  const demoWindow = window !== undefined ? window() : undefined;

  return (
    // preview-start
    <AppProvider
      navigation={NAVIGATION}
      router={router}
      theme={demoTheme}
      window={demoWindow}
    >
      <DashboardLayout>
        <DemoPageContent pathname={pathname} />
      </DashboardLayout>
    </AppProvider>
    // preview-end
  );
}

DashboardLayoutBasic.propTypes = {
  /**
   * Injected by the documentation to work in an iframe.
   * Remove this when copying and pasting into your project.
   */
  window: PropTypes.func,
};

export default DashboardLayoutBasic;
